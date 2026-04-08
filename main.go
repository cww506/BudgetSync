package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/xuri/excelize/v2"
	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS mkt_bd_budget (
    control_center TEXT NOT NULL,
    sub_market     TEXT NOT NULL,
    project_number TEXT NOT NULL,
    task_number    TEXT NOT NULL,
    period         TEXT NOT NULL,
    ytd_hours      REAL,
    ytd_amount     REAL,
    budget_hours   REAL,
    budget_amount  REAL,
    jtd_hours      REAL,
    jtd_amount     REAL,
    PRIMARY KEY (control_center, sub_market, project_number, task_number, period)
);`

const upsertSQL = `
INSERT INTO mkt_bd_budget
    (control_center, sub_market, project_number, task_number, period,
     ytd_hours, ytd_amount, budget_hours, budget_amount, jtd_hours, jtd_amount)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(control_center, sub_market, project_number, task_number, period)
DO UPDATE SET
    ytd_hours    = excluded.ytd_hours,
    ytd_amount   = excluded.ytd_amount,
    budget_hours = excluded.budget_hours,
    budget_amount= excluded.budget_amount,
    jtd_hours    = excluded.jtd_hours,
    jtd_amount   = excluded.jtd_amount;`

var periodRe = regexp.MustCompile(`^(\d{2}-\d{4})`)

// column headers as they appear in the Excel file
const (
	colControlCenter = "Header Field 1"
	colSubMarket     = "Header Field 11"
	colProjectNumber = "Header Field 12"
	colTaskNumber    = "Header Field 13"
	colYTDHours      = "YTD Hours"
	colYTDAmount     = "YTD Amount"
	colBudgetHours   = "Budget Hours"
	colBudgetAmount  = "Budget Amount"
	colJTDHours      = "JTD Hours"
	colJTDAmount     = "JTD  Amount" // double space in source
)

func main() {
	dir := flag.String("dir", "", "Directory containing .xlsx files (required)")
	dbPath := flag.String("db", "", "Path to SQLite database file (required)")
	flag.Parse()

	if *dir == "" || *dbPath == "" {
		flag.Usage()
		log.Fatal("both --dir and --db are required")
	}

	db, err := sql.Open("sqlite", *dbPath)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(schema); err != nil {
		log.Fatalf("create schema: %v", err)
	}

	err = filepath.WalkDir(*dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || strings.ToLower(filepath.Ext(path)) != ".xlsx" {
			return nil
		}
		if procErr := processFile(db, path); procErr != nil {
			log.Printf("SKIP %s: %v", filepath.Base(path), procErr)
		}
		return nil
	})
	if err != nil {
		log.Fatalf("walk directory: %v", err)
	}
}

func processFile(db *sql.DB, path string) error {
	name := filepath.Base(path)

	period := periodRe.FindString(name)
	if period == "" {
		return fmt.Errorf("cannot extract MM-YYYY period from filename")
	}

	f, err := excelize.OpenFile(path)
	if err != nil {
		return fmt.Errorf("open excel: %w", err)
	}
	defer f.Close()

	sheets := f.GetSheetList()
	if len(sheets) == 0 {
		return fmt.Errorf("no sheets found")
	}

	rows, err := f.GetRows(sheets[0])
	if err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	if len(rows) < 2 {
		return fmt.Errorf("no data rows found")
	}

	// build header index from row 0
	colIdx := make(map[string]int)
	for i, h := range rows[0] {
		colIdx[strings.TrimSpace(h)] = i
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(upsertSQL)
	if err != nil {
		return fmt.Errorf("prepare statement: %w", err)
	}
	defer stmt.Close()

	inserted, skipped := 0, 0
	for rowNum, row := range rows[1:] {
		rec, err := parseRow(row, colIdx, period)
		if err != nil {
			log.Printf("  row %d skipped: %v", rowNum+2, err)
			skipped++
			continue
		}
		_, err = stmt.Exec(
			rec.controlCenter, rec.subMarket, rec.projectNumber, rec.taskNumber, rec.period,
			rec.ytdHours, rec.ytdAmount, rec.budgetHours, rec.budgetAmount,
			rec.jtdHours, rec.jtdAmount,
		)
		if err != nil {
			log.Printf("  row %d upsert error: %v", rowNum+2, err)
			skipped++
			continue
		}
		inserted++
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	log.Printf("%-50s period=%-7s  upserted=%d  skipped=%d", name, period, inserted, skipped)
	return nil
}

type record struct {
	controlCenter string
	subMarket     string
	projectNumber string
	taskNumber    string
	period        string
	ytdHours      float64
	ytdAmount     float64
	budgetHours   float64
	budgetAmount  float64
	jtdHours      float64
	jtdAmount     float64
}

func parseRow(row []string, colIdx map[string]int, period string) (record, error) {
	cell := func(header string) string {
		idx, ok := colIdx[header]
		if !ok || idx >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[idx])
	}

	controlCenter := strings.TrimSpace(strings.TrimPrefix(cell(colControlCenter), "Control Center: "))
	subMarket := strings.TrimSpace(strings.TrimPrefix(cell(colSubMarket), "Sub Sector: "))
	projectNumber := strings.TrimSpace(strings.TrimPrefix(cell(colProjectNumber), "Project Number: "))
	taskNumber := strings.TrimSpace(strings.TrimPrefix(cell(colTaskNumber), "Task Number: "))

	if controlCenter == "" || projectNumber == "" {
		return record{}, fmt.Errorf("missing required fields (control_center or project_number)")
	}

	return record{
		controlCenter: controlCenter,
		subMarket:     subMarket,
		projectNumber: projectNumber,
		taskNumber:    taskNumber,
		period:        period,
		ytdHours:      parseFloat(cell(colYTDHours)),
		ytdAmount:     parseFloat(cell(colYTDAmount)),
		budgetHours:   parseFloat(cell(colBudgetHours)),
		budgetAmount:  parseFloat(cell(colBudgetAmount)),
		jtdHours:      parseFloat(cell(colJTDHours)),
		jtdAmount:     parseFloat(cell(colJTDAmount)),
	}, nil
}

func parseFloat(s string) float64 {
	s = strings.ReplaceAll(s, ",", "")
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

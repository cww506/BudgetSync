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
DROP TABLE IF EXISTS mkt_bd_budget;
CREATE TABLE mkt_bd_budget (
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

const insertSQL = `
INSERT INTO mkt_bd_budget
    (control_center, sub_market, project_number, task_number, period,
     ytd_hours, ytd_amount, budget_hours, budget_amount, jtd_hours, jtd_amount)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

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

	// Bulk-load pragmas — removes fsync overhead; safe since a crash just means re-running.
	pragmas := []string{
		"PRAGMA synchronous = OFF",
		"PRAGMA journal_mode = OFF",
		"PRAGMA temp_store = MEMORY",
		"PRAGMA cache_size = -64000",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			log.Fatalf("pragma %q: %v", p, err)
		}
	}

	if _, err := db.Exec(schema); err != nil {
		log.Fatalf("create schema: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("begin transaction: %v", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		log.Fatalf("prepare statement: %v", err)
	}
	defer stmt.Close()

	err = filepath.WalkDir(*dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || strings.ToLower(filepath.Ext(path)) != ".xlsx" {
			return nil
		}
		if procErr := processFile(stmt, path); procErr != nil {
			log.Printf("SKIP %s: %v", filepath.Base(path), procErr)
		}
		return nil
	})
	if err != nil {
		log.Fatalf("walk directory: %v", err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("commit: %v", err)
	}
}

func processFile(stmt *sql.Stmt, path string) error {
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
			log.Printf("  row %d insert error: %v", rowNum+2, err)
			skipped++
			continue
		}
		inserted++
	}

	log.Printf("%-50s period=%-7s  inserted=%d  skipped=%d", name, period, inserted, skipped)
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

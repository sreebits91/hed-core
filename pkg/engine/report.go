package engine

import (
	"fmt"
	"io"
	"time"

	"github.com/jung-kurt/gofpdf"
)

type SummaryReport struct {
	EngineName     string
	TotalCommitted uint64
	TotalFailed    uint64
	DurationSec    float64
	AverageTPS     float64
	AvgLatencyUs   int64
}

// GenerateTPSReport writes a formatted PDF report directly to an io.Writer (e.g. http.ResponseWriter).
func GenerateTPSReport(w io.Writer, r SummaryReport) error {
	pdf := gofpdf.New("P", "mm", "A4", "")
	pdf.AddPage()

	// Title Header
	pdf.SetFont("Arial", "B", 20)
	pdf.Cell(190, 10, "HED-Core Engine Performance Report")
	pdf.Ln(12)

	// Timestamp
	pdf.SetFont("Arial", "I", 10)
	pdf.Cell(190, 6, fmt.Sprintf("Generated on: %s", time.Now().Format("2006-01-02 15:04:05 MST")))
	pdf.Ln(10)

	// Section 1: Context
	pdf.SetFont("Arial", "B", 14)
	pdf.Cell(190, 8, "1. System Context")
	pdf.Ln(8)

	pdf.SetFont("Arial", "", 11)
	pdf.Cell(60, 6, "Active Storage Engine:")
	pdf.Cell(130, 6, r.EngineName)
	pdf.Ln(6)
	pdf.Cell(60, 6, "Execution Duration:")
	pdf.Cell(130, 6, fmt.Sprintf("%.2f seconds", r.DurationSec))
	pdf.Ln(10)

	// Section 2: Metrics Table
	pdf.SetFont("Arial", "B", 14)
	pdf.Cell(190, 8, "2. Throughput & Execution Summary")
	pdf.Ln(8)

	pdf.SetFont("Arial", "B", 11)
	pdf.SetFillColor(230, 230, 230)
	pdf.CellFormat(95, 7, "Metric", "1", 0, "L", true, 0, "")
	pdf.CellFormat(95, 7, "Value", "1", 1, "L", true, 0, "")

	pdf.SetFont("Arial", "", 11)
	metrics := [][]string{
		{"Committed Transactions", fmt.Sprintf("%d", r.TotalCommitted)},
		{"Failed Transactions", fmt.Sprintf("%d", r.TotalFailed)},
		{"Average Throughput (TPS)", fmt.Sprintf("%.2f ops/sec", r.AverageTPS)},
		{"Average Latency", fmt.Sprintf("%d us", r.AvgLatencyUs)},
	}

	for _, m := range metrics {
		pdf.CellFormat(95, 7, m[0], "1", 0, "L", false, 0, "")
		pdf.CellFormat(95, 7, m[1], "1", 1, "L", false, 0, "")
	}

	pdf.Ln(12)

	// Status Banner
	pdf.SetFont("Arial", "B", 12)
	if r.TotalFailed == 0 {
		pdf.SetTextColor(0, 128, 0)
		pdf.Cell(190, 8, "STATUS: Execution completed with zero failures.")
	} else {
		pdf.SetTextColor(200, 0, 0)
		pdf.Cell(190, 8, fmt.Sprintf("STATUS: Completed with %d errors.", r.TotalFailed))
	}

	return pdf.Output(w)
}

package chaos

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
)

func ms(d time.Duration) string {
	if d == 0 {
		return "-"
	}
	if d < 10*time.Millisecond {
		return fmt.Sprintf("%.1f ms", float64(d.Microseconds())/1000)
	}
	return fmt.Sprintf("%d ms", d.Milliseconds())
}

func codes(m map[string]int) string {
	if len(m) == 0 {
		return "-"
	}
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	var parts []string
	for _, k := range ks {
		parts = append(parts, fmt.Sprintf("%s×%d", k, m[k]))
	}
	return strings.Join(parts, ", ")
}

// WriteTable prints the results table and per-scenario details.
func WriteTable(w io.Writer, results []*Result) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SCENARIO\tACKED\tLOST\tDUPS\tORDER\tSTALL surviving\tSTALL global\tSTALL affected\tMSG/S\tMAX ERR\tPROBE MAX\tAUDIT b/d/a\tRESULT")
	for _, r := range results {
		verdict := "PASS"
		if !r.Pass {
			verdict = "FAIL"
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%s\t%s\t%s\t%.0f\t%s\t%s\t%s/%s/%s\t%s\n",
			r.Scenario, r.Acked, r.Lost, r.Duplicates, r.OrderViolations,
			ms(r.StallSurviving), ms(r.StallGlobal), ms(r.StallAffected), r.Throughput,
			ms(r.MaxErrLatency), ms(r.ProbeMax), r.AuditBefore, r.AuditDuring, r.AuditAfter, verdict)
	}
	tw.Flush()
	for _, r := range results {
		fmt.Fprintf(w, "\n[%s] %s\n", r.Scenario, r.Description)
		for _, n := range r.Notes {
			fmt.Fprintf(w, "  - %s\n", n)
		}
		fmt.Fprintf(w, "  - failed attempts (all retried): %d  %s\n", r.FailedAttempts, codes(r.ErrorCodes))
		fmt.Fprintf(w, "  - cross-region probe answers: %s\n", codes(r.ProbeCodes))
		for _, f := range r.Failures {
			fmt.Fprintf(w, "  ! %s\n", f)
		}
	}
}

// WriteMarkdown writes the same results as a Markdown table (for the README).
func WriteMarkdown(w io.Writer, results []*Result) {
	fmt.Fprintln(w, "| Scenario | Acked | Lost | Dups | Order | Max stall surviving | Max stall global | Max stall affected | msg/s | Slowest error | Slowest probe | Audit before/during/after | Result |")
	fmt.Fprintln(w, "|---|---|---|---|---|---|---|---|---|---|---|---|---|")
	for _, r := range results {
		verdict := "✅ PASS"
		if !r.Pass {
			verdict = "❌ FAIL: " + strings.Join(r.Failures, "; ")
		}
		fmt.Fprintf(w, "| %s | %d | %d | %d | %d | %s | %s | %s | %.0f | %s | %s | %s/%s/%s | %s |\n",
			r.Scenario, r.Acked, r.Lost, r.Duplicates, r.OrderViolations,
			ms(r.StallSurviving), ms(r.StallGlobal), ms(r.StallAffected), r.Throughput,
			ms(r.MaxErrLatency), ms(r.ProbeMax), r.AuditBefore, r.AuditDuring, r.AuditAfter, verdict)
	}
	for _, r := range results {
		fmt.Fprintf(w, "\n**%s** — %s\n", r.Scenario, r.Description)
		for _, n := range r.Notes {
			fmt.Fprintf(w, "- %s\n", n)
		}
		fmt.Fprintf(w, "- failed attempts (all retried): %d — %s\n", r.FailedAttempts, codes(r.ErrorCodes))
		fmt.Fprintf(w, "- cross-region probe answers: %s\n", codes(r.ProbeCodes))
	}
}

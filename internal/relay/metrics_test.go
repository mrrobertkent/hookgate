package relay

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mrrobertkent/hookgate/internal/testutil"
)

const infoSeries = `hookgate_relay_info{stream="WEBHOOK_EVENTS",consumer="app"} 1`

// scrape returns the relay's /metrics body.
func scrape(t *testing.T, r *Relay) string {
	t.Helper()
	rec := httptest.NewRecorder()
	r.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics answered %d", rec.Code)
	}
	return rec.Body.String()
}

// checkInfo fails unless body holds the info family's metadata and exactly one series, infoSeries.
func checkInfo(t *testing.T, body string) {
	t.Helper()
	if !strings.Contains(body, "# TYPE hookgate_relay_info gauge\n") || !strings.Contains(body, "# HELP hookgate_relay_info ") {
		t.Errorf("info metadata missing:\n%s", body)
	}
	var series []string
	for line := range strings.SplitSeq(body, "\n") {
		if strings.HasPrefix(line, "hookgate_relay_info") {
			series = append(series, line)
		}
	}
	if len(series) != 1 || series[0] != infoSeries {
		t.Errorf("info series %q, want [%q]", series, infoSeries)
	}
}

func TestInfoMetricBeforeRun(t *testing.T) {
	e := newEnv(t)
	r, err := New(e.config(t, "app", helperHandler(t, "exit", "0")), testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	if r.notReady() == nil {
		t.Fatal("relay ready before it ran")
	}
	body := scrape(t, r)
	checkInfo(t, body)
	if strings.Contains(body, "hookgate_relay_deliveries_total{") {
		t.Errorf("deliveries reported before any delivery:\n%s", body)
	}
}

func TestInfoMetricWhileUnready(t *testing.T) {
	e := newEnv(t)
	// No consumer exists, so the relay runs but never becomes ready.
	r := start(t, e.config(t, "app", helperHandler(t, "exit", "0")), false)
	testutil.Eventually(t, 10*time.Second, func() bool {
		err := r.notReady()
		return err != nil && err.Error() != "not checked yet"
	}, "readiness never checked")
	checkInfo(t, scrape(t, r.Relay))
	if v := metric(r.Relay, "hookgate_relay_ready"); v == 1 {
		t.Errorf("hookgate_relay_ready %v while unready", v)
	}
}

// TestInfoMetricSharedConsumer runs two relays on one durable consumer: each reports the same single series, so
// the scrape target's labels are what tell them apart.
func TestInfoMetricSharedConsumer(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "app")
	a := start(t, e.config(t, "app", helperHandler(t, "exit", "0")), true)
	b := start(t, e.config(t, "app", helperHandler(t, "exit", "0")), true)
	seq := e.publish(t, pub{body: []byte("{}")})
	e.waitSettled(t, "app", seq)
	for _, r := range []*running{a, b} {
		checkInfo(t, scrape(t, r.Relay))
	}
	if metric(a.Relay, outcome(outcomeAcked)) != 1 && metric(b.Relay, outcome(outcomeAcked)) != 1 {
		t.Error("no relay acked the event")
	}
}

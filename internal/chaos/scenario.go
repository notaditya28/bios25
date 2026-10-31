package chaos

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/audit"
	"github.com/notaditya28/hydra-stream/internal/client"
)

type Options struct {
	Batch     int           // messages per produce request
	Interval  time.Duration // one batch per stream every Interval
	Warmup    time.Duration // load before the fault
	Hold      time.Duration // fault duration
	Recover   time.Duration // load after every stream has recovered
	TrustFile string        // audit key pinning
	Faulted   string        // region the region-level faults target
}

func DefaultOptions() Options {
	return Options{Batch: 20, Interval: 10 * time.Millisecond, Warmup: 3 * time.Second, Hold: 6 * time.Second,
		Recover: 2 * time.Second, Faulted: "IN"}
}

type Result struct {
	Scenario        string         `json:"scenario"`
	Description     string         `json:"description"`
	Acked           int            `json:"acked"`
	Lost            int            `json:"lost"`
	Duplicates      int            `json:"duplicates"`
	OrderViolations int            `json:"order_violations"`
	Unacked         int            `json:"in_log_not_acked"` // written but the ack never arrived (e.g. stop mid-batch)
	StallSurviving  time.Duration  `json:"stall_surviving_ns"`
	StallGlobal     time.Duration  `json:"stall_global_ns"`
	StallAffected   time.Duration  `json:"stall_affected_ns"`
	Throughput      float64        `json:"throughput_msgs_per_s"`
	FailedAttempts  int            `json:"failed_attempts"`
	MaxErrLatency   time.Duration  `json:"max_error_latency_ns"`
	ErrorCodes      map[string]int `json:"error_codes"`
	ProbeMax        time.Duration  `json:"probe_max_ns"`
	ProbeCodes      map[string]int `json:"probe_codes"`
	AuditBefore     string         `json:"audit_before"`
	AuditDuring     string         `json:"audit_during"`
	AuditAfter      string         `json:"audit_after"`
	Notes           []string       `json:"notes"`
	Pass            bool           `json:"pass"`
	Failures        []string       `json:"failures"`
}

type env struct {
	p     *Procs
	opts  Options
	notes []string
	lat   *LatencyProbe
	check func() error // extra pass criterion
}

func (e *env) note(format string, args ...any) {
	e.notes = append(e.notes, fmt.Sprintf(format, args...))
	slog.Info("scenario", "note", fmt.Sprintf(format, args...))
}

type Scenario struct {
	Name        string
	Description string
	Affected    bool                                    // the faulted region's resident stream is expected to be disrupted
	Prepare     func(ctx context.Context, e *env) error // before load starts
	Fault       func(e *env) error
	Heal        func(e *env) error
}

func Scenarios() []Scenario {
	return []Scenario{
		{
			Name: "baseline", Description: "no fault",
			Fault: func(*env) error { return nil }, Heal: func(*env) error { return nil },
		},
		killBroker(),
		killZone(),
		{
			Name: "kill-region", Description: "kill every broker of the faulted region", Affected: true,
			Fault: func(e *env) error {
				for _, b := range e.p.Region(e.opts.Faulted) {
					e.p.Kill(b.ID)
				}
				e.note("killed all of %s", e.opts.Faulted)
				return nil
			},
			Heal: func(e *env) error {
				for _, b := range e.p.Region(e.opts.Faulted) {
					if err := e.p.Start(b.ID); err != nil {
						return err
					}
				}
				return nil
			},
		},
		{
			Name: "isolate-region", Description: "faulted region alive but cut off from the other regions", Affected: false,
			Fault: func(e *env) error {
				r := e.opts.Faulted
				e.p.SetNet(func(a, b BrokerRef) bool { return (a.Region == r) != (b.Region == r) }, nil)
				e.note("isolated %s (both directions)", r)
				return nil
			},
			Heal: func(e *env) error { e.p.HealNet(); return nil },
		},
		slowLink(),
	}
}

func killBroker() Scenario {
	var victim BrokerRef
	return Scenario{
		Name: "kill-broker", Description: "kill the leader of the faulted region's resident partition", Affected: true,
		Fault: func(e *env) error {
			topic := strings.ToLower(e.opts.Faulted) + "-orders"
			b, ok := e.p.Leader(topic, 0)
			if !ok {
				return fmt.Errorf("no leader for %s/0", topic)
			}
			victim = b
			e.p.Kill(b.ID)
			e.note("killed %s (leader of %s/0)", b.ID, topic)
			return nil
		},
		Heal: func(e *env) error { return e.p.Start(victim.ID) },
	}
}

func killZone() Scenario {
	var victim BrokerRef
	return Scenario{
		Name: "kill-zone", Description: "kill the zone hosting the faulted region's controller", Affected: true,
		Fault: func(e *env) error {
			b, ok := e.p.Controller(e.opts.Faulted)
			if !ok {
				return fmt.Errorf("no controller in %s", e.opts.Faulted)
			}
			victim = b
			e.p.Kill(b.ID)
			e.note("killed zone %s (broker %s, the %s controller)", b.Zone, b.ID, e.opts.Faulted)
			return nil
		},
		Heal: func(e *env) error { return e.p.Start(victim.ID) },
	}
}

func slowLink() Scenario {
	var ra, rb, observer, leaderID string
	const delay = 150
	return Scenario{
		Name: "slow-link", Description: fmt.Sprintf("add %d ms each way on the link between the global leader's region and another", delay),
		Prepare: func(ctx context.Context, e *env) error {
			l, ok := e.p.Leader("prices", 0)
			if !ok {
				return fmt.Errorf("no leader for prices/0")
			}
			ra, leaderID = l.Region, l.ID
			for _, r := range e.p.Regions() {
				if r != ra {
					rb = r
					break
				}
			}
			observer = e.p.Region(rb)[0].ID
			e.lat = &LatencyProbe{Topic: "prices", Partition: 0, Addr: e.p.Broker(observer).Addr}
			return nil
		},
		Fault: func(e *env) error {
			e.p.SetNet(nil, func(a, b BrokerRef) int64 {
				if (a.Region == ra && b.Region == rb) || (a.Region == rb && b.Region == ra) {
					return delay
				}
				return 0
			})
			e.note("slowed %s<->%s by %d ms each way; observing prices/0 (leader %s) from %s", ra, rb, delay, leaderID, observer)
			e.check = func() error {
				via, rtt := "", 0.0
				for _, l := range e.p.Routes(observer) {
					if l.To == leaderID {
						via, rtt = l.Via, l.RTTms
					}
				}
				e.note("during the fault %s reached %s via %q (direct RTT measured %.0f ms)", observer, leaderID, via, rtt)
				if via == "" {
					return fmt.Errorf("global traffic was not rerouted around the slow link")
				}
				return nil
			}
			return nil
		},
		Heal: func(e *env) error { e.p.HealNet(); return nil },
	}
}

// Setup creates the topics the scenarios use: one resident topic per region and a global one.
func Setup(ctx context.Context, p *Procs) error {
	create := func(addr string, cfg api.TopicConfig) error {
		deadline := time.Now().Add(30 * time.Second)
		for {
			_, err := client.New(addr).CreateTopic(ctx, cfg)
			if err == nil {
				return nil
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("create %s: %w", cfg.Name, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	for _, r := range p.Regions() {
		if err := create(p.Region(r)[0].Addr, api.TopicConfig{Name: strings.ToLower(r) + "-orders", Partitions: 1, Residency: r}); err != nil {
			return err
		}
	}
	return create(p.Brokers()[0].Addr, api.TopicConfig{Name: "prices", Partitions: len(p.Regions()), Residency: api.ResidencyGlobal})
}

func addrs(bs []BrokerRef) []string {
	var out []string
	for _, b := range bs {
		out = append(out, b.Addr)
	}
	return out
}

func runAudit(ctx context.Context, p *Procs, trust string) string {
	rep, err := audit.Run(ctx, p.Cluster, audit.Options{DataRoot: p.DataRoot, Online: true, TrustFile: trust})
	if err != nil {
		return "ERROR"
	}
	if rep.Verdict == "PASS" {
		return "PASS"
	}
	return fmt.Sprintf("FAIL(%d)", rep.Violations)
}

// RunScenario runs one scenario under load and verifies the result.
func RunScenario(ctx context.Context, p *Procs, sc Scenario, opts Options) (*Result, error) {
	e := &env{p: p, opts: opts}
	res := &Result{Scenario: sc.Name, Description: sc.Description, ErrorCodes: map[string]int{}, ProbeCodes: map[string]int{}}
	slog.Info("scenario starting", "name", sc.Name)
	if sc.Prepare != nil {
		if err := sc.Prepare(ctx, e); err != nil {
			return nil, err
		}
	}

	var streams []*Stream
	for _, r := range p.Regions() {
		streams = append(streams, &Stream{Name: sc.Name + "-" + strings.ToLower(r), Topic: strings.ToLower(r) + "-orders",
			Residency: r, Home: r, Addrs: addrs(p.Region(r))})
	}
	for i, r := range p.Regions() {
		as := addrs(p.Region(r))
		for _, o := range p.Regions() {
			if o != r {
				as = append(as, addrs(p.Region(o))...)
			}
		}
		streams = append(streams, &Stream{Name: fmt.Sprintf("%s-g%d", sc.Name, i), Topic: "prices", Partition: i,
			Residency: api.ResidencyGlobal, Home: r, Addrs: as})
	}
	var probes []*Probe
	for _, r := range p.Regions() {
		var via []string
		for _, o := range p.Regions() {
			if o != r {
				via = append(via, addrs(p.Region(o))...)
			}
		}
		probes = append(probes, &Probe{Topic: strings.ToLower(r) + "-orders", Region: r, Via: via})
	}

	lctx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for _, s := range streams {
		wg.Add(1)
		go func() { defer wg.Done(); s.Run(lctx, opts.Batch, opts.Interval) }()
	}
	for _, pr := range probes {
		wg.Add(1)
		go func() { defer wg.Done(); pr.Run(lctx, 250*time.Millisecond) }()
	}
	if e.lat != nil {
		wg.Add(1)
		go func() { defer wg.Done(); e.lat.Run(lctx, 50*time.Millisecond) }()
	}
	started := time.Now()

	sleep(ctx, opts.Warmup)
	res.AuditBefore = runAudit(ctx, p, opts.TrustFile)
	faultAt := time.Now()
	if err := sc.Fault(e); err != nil {
		stop()
		wg.Wait()
		return nil, err
	}
	sleep(ctx, opts.Hold/2)
	res.AuditDuring = runAudit(ctx, p, opts.TrustFile)
	sleep(ctx, opts.Hold-opts.Hold/2)
	if e.check != nil {
		if err := e.check(); err != nil {
			res.Failures = append(res.Failures, err.Error())
		}
	}
	if e.lat != nil {
		e.note("prices/0 fetch p50 from %s: %v before the fault, %v in the second half of it",
			e.lat.Addr, e.lat.P50(started, faultAt).Round(100*time.Microsecond), e.lat.P50(faultAt.Add(opts.Hold/2), time.Now()).Round(100*time.Microsecond))
	}
	if err := sc.Heal(e); err != nil {
		stop()
		wg.Wait()
		return nil, err
	}
	healAt := time.Now()

	// Keep the load running until every stream has been acknowledged again after the heal.
	deadline := healAt.Add(30 * time.Second)
	for time.Now().Before(deadline) {
		done := true
		for _, s := range streams {
			if !s.LastAck().After(healAt) {
				done = false
			}
		}
		if done {
			break
		}
		sleep(ctx, 100*time.Millisecond)
	}
	e.note("all streams acknowledged again %v after the heal", time.Since(healAt).Round(time.Millisecond))
	sleep(ctx, opts.Recover)
	stop()
	wg.Wait()
	elapsed := time.Since(started)

	if err := p.WaitHealthy(ctx, 30*time.Second); err != nil {
		res.Failures = append(res.Failures, err.Error())
	}
	verify(ctx, res, streams, sc, opts)
	res.Throughput = float64(res.Acked) / elapsed.Seconds()
	for _, pr := range probes {
		for _, a := range pr.Answers() {
			res.ProbeCodes[a.Code]++
			res.ProbeMax = max(res.ProbeMax, a.Took)
		}
	}
	res.AuditAfter = runAudit(ctx, p, opts.TrustFile)
	res.Notes = e.notes
	judge(res)
	return res, nil
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// consume reads a partition's committed log through any of addrs, retrying while the cluster settles.
func consume(ctx context.Context, as []string, topic string, part int) (map[uint64]string, []uint64, error) {
	deadline := time.Now().Add(30 * time.Second)
	for {
		vals, order, err := consumeOnce(ctx, as, topic, part)
		if err == nil || time.Now().After(deadline) {
			return vals, order, err
		}
		sleep(ctx, 200*time.Millisecond)
	}
}

func consumeOnce(ctx context.Context, as []string, topic string, part int) (map[uint64]string, []uint64, error) {
	vals := map[uint64]string{}
	var order []uint64
	var off uint64
	for {
		var resp api.FetchResponse
		var err error
		for _, a := range as {
			cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			resp, err = client.New(a).Fetch(cctx, api.FetchRequest{Topic: topic, Partition: part, Offset: off, MaxRecords: 10000, MaxBytes: 8 << 20})
			cancel()
			if err == nil {
				break
			}
		}
		if err != nil {
			return nil, nil, err
		}
		for _, r := range resp.Records {
			vals[r.Offset] = r.Value
			order = append(order, r.Offset)
		}
		if len(resp.Records) == 0 && resp.NextOffset >= resp.HighWatermark {
			return vals, order, nil
		}
		off = resp.NextOffset
	}
}

func verify(ctx context.Context, res *Result, streams []*Stream, sc Scenario, opts Options) {
	type key struct {
		topic string
		part  int
	}
	logs := map[key]map[uint64]string{}
	orders := map[key][]uint64{}
	for _, s := range streams {
		k := key{s.Topic, s.Partition}
		if _, ok := logs[k]; ok {
			continue
		}
		vals, order, err := consume(ctx, s.Addrs, s.Topic, s.Partition)
		if err != nil {
			res.Failures = append(res.Failures, fmt.Sprintf("could not read %s/%d back: %v", s.Topic, s.Partition, err))
			continue
		}
		logs[k], orders[k] = vals, order
	}
	for _, s := range streams {
		k := key{s.Topic, s.Partition}
		log, ok := logs[k]
		if !ok {
			continue
		}
		acks := s.Acks()
		res.Acked += len(acks)
		lost := 0
		for _, a := range acks {
			if log[a.Offset] != a.Value {
				lost++
			}
		}
		res.Lost += lost
		count := map[string]int{}
		prev, inLog := -1, 0
		for _, off := range orders[k] {
			v := log[off]
			n, mine := s.seqOf(v)
			if !mine {
				continue
			}
			inLog++
			count[v]++
			if n <= prev {
				res.OrderViolations++
			}
			prev = n
		}
		dups := 0
		for _, c := range count {
			dups += c - 1
		}
		res.Duplicates += dups
		res.Unacked += inLog - dups - (len(acks) - lost)

		stall := s.MaxStall()
		switch {
		case s.Residency == api.ResidencyGlobal:
			res.StallGlobal = max(res.StallGlobal, stall)
		case sc.Affected && s.Home == opts.Faulted:
			res.StallAffected = max(res.StallAffected, stall)
		case s.Home == opts.Faulted && !sc.Affected:
			// isolated region's own resident stream: it should keep flowing, count it as surviving
			res.StallSurviving = max(res.StallSurviving, stall)
		default:
			res.StallSurviving = max(res.StallSurviving, stall)
		}
		for _, f := range s.Fails() {
			res.FailedAttempts++
			res.ErrorCodes[f.Code+reasonSuffix(f.Reason)]++
			res.MaxErrLatency = max(res.MaxErrLatency, f.Took)
		}
	}
}

func reasonSuffix(r string) string {
	if r == "" {
		return ""
	}
	return "/" + r
}

// judge applies the definition of done.
func judge(r *Result) {
	if r.Lost > 0 {
		r.Failures = append(r.Failures, fmt.Sprintf("%d acknowledged messages lost", r.Lost))
	}
	if r.Duplicates > 0 {
		r.Failures = append(r.Failures, fmt.Sprintf("%d duplicates", r.Duplicates))
	}
	if r.OrderViolations > 0 {
		r.Failures = append(r.Failures, fmt.Sprintf("%d order violations", r.OrderViolations))
	}
	if r.StallSurviving >= time.Second {
		r.Failures = append(r.Failures, fmt.Sprintf("surviving traffic stalled %v (target < 1 s)", r.StallSurviving.Round(time.Millisecond)))
	}
	if r.ProbeMax >= 500*time.Millisecond {
		r.Failures = append(r.Failures, fmt.Sprintf("an explicit cross-region error took %v (target < 500 ms)", r.ProbeMax.Round(time.Millisecond)))
	}
	if r.MaxErrLatency >= 2500*time.Millisecond {
		r.Failures = append(r.Failures, fmt.Sprintf("a failed request hung for %v", r.MaxErrLatency.Round(time.Millisecond)))
	}
	for _, a := range []struct{ when, v string }{{"before", r.AuditBefore}, {"during", r.AuditDuring}, {"after", r.AuditAfter}} {
		if a.v != "PASS" {
			r.Failures = append(r.Failures, "residency audit "+a.when+": "+a.v)
		}
	}
	r.Pass = len(r.Failures) == 0
}

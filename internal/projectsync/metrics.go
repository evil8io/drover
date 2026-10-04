package projectsync

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// meterName and tracerName are the instrumentation scope of the sync metrics
// and the sync spans.
const (
	meterName  = "github.com/evil8io/drover/internal/projectsync"
	tracerName = meterName
)

const (
	outcomeOK    = "ok"
	outcomeError = "error"
	// outcomeMissingMount is an OpenBao config write to a mount that does not
	// exist yet.
	outcomeMissingMount = "missing_mount"

	// originReconcile, originWatch, and originProject name the path that
	// patched a namespace. originProject is a namespace that the lister found
	// after a project change.
	originReconcile = "reconcile"
	originWatch     = "watch"
	originProject   = "project"

	// kindNamespace and kindProject name the objects of a watch.
	kindNamespace = "namespace"
	kindProject   = "project"
)

// metrics has the instruments that record the outcome of a reconcile run.
type metrics struct {
	reconciles metric.Int64Counter
	patched    metric.Int64Counter
	errors     metric.Int64Counter
	duration   metric.Float64Histogram
	events     metric.Int64Counter
	watches    metric.Int64UpDownCounter
	changed    metric.Int64Counter
	accounts   metric.Int64Counter
	openbao    metric.Int64Counter
	openbaoSet metric.Int64Counter

	// trustMu guards trust, the points of the trust gauge by project.
	trustMu sync.Mutex
	trust   map[projectRef][]trustPoint
}

// trustPoint is one point of the trust gauge.
type trustPoint struct {
	value int64
	attrs attribute.Set
}

// newMetrics creates the instruments of the syncer, on the meter that
// provider gives for meterName.
func newMetrics(provider metric.MeterProvider) (*metrics, error) {
	meter := provider.Meter(meterName)

	reconciles, err := meter.Int64Counter("drover.sync.reconciles",
		metric.WithDescription("Reconcile runs that the syncer completes."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	patched, err := meter.Int64Counter("drover.sync.namespaces.patched",
		metric.WithDescription("Namespaces that the syncer patches."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	errs, err := meter.Int64Counter("drover.sync.errors",
		metric.WithDescription("Errors that a reconcile run logs."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram("drover.sync.duration",
		metric.WithDescription("Duration of a reconcile run."),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	events, err := meter.Int64Counter("drover.sync.events",
		metric.WithDescription("Watch events that the syncer reads."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	watches, err := meter.Int64UpDownCounter("drover.sync.watches.open",
		metric.WithDescription("Watch streams that are open."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	changed, err := meter.Int64Counter("drover.sync.projects.changed",
		metric.WithDescription("Project changes that the project watch applies."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}

	accounts, err := meter.Int64Counter("drover.sync.accounts.changes",
		metric.WithDescription("Writes of the service accounts, their namespaces, and their bindings."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	openbao, err := meter.Int64Counter("drover.sync.openbao.writes",
		metric.WithDescription("Writes of the Kubernetes secrets engine config of a cluster into OpenBao."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	openbaoSet, err := meter.Int64Counter("drover.sync.openbao.changes",
		metric.WithDescription("Writes of the OpenBao mounts, roles, and policies."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}

	m := &metrics{
		reconciles: reconciles,
		patched:    patched,
		errors:     errs,
		duration:   duration,
		events:     events,
		watches:    watches,
		changed:    changed,
		accounts:   accounts,
		openbao:    openbao,
		openbaoSet: openbaoSet,
		trust:      make(map[projectRef][]trustPoint),
	}
	if _, err := meter.Int64ObservableGauge("drover.sync.trust.statements.ready",
		metric.WithDescription("The readiness of each trust statement of a project: 1 for a ready statement, 0 for a statement that is not ready."),
		metric.WithUnit("1"),
		metric.WithInt64Callback(m.observeTrust)); err != nil {
		return nil, err
	}
	return m, nil
}

// reconcileDone records one finished reconcile run, with its outcome and its
// duration in seconds.
func (m *metrics) reconcileDone(ctx context.Context, outcome string, duration time.Duration) {
	m.reconciles.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
	m.duration.Record(ctx, duration.Seconds())
}

// namespacePatched records one namespace that the syncer patches. origin is
// the path that found the namespace.
func (m *metrics) namespacePatched(ctx context.Context, origin string) {
	m.patched.Add(ctx, 1, metric.WithAttributes(attribute.String("origin", origin)))
}

// watchEvent records one event of a watch. cluster is the cluster of the
// stream, and kind names its objects.
func (m *metrics) watchEvent(ctx context.Context, cluster, kind, eventType string) {
	m.events.Add(ctx, 1, metric.WithAttributes(
		attribute.String("drover.cluster", cluster),
		attribute.String("kind", kind),
		attribute.String("type", eventType)))
}

// watchOpened and watchClosed track the open watch streams per cluster and
// kind.
func (m *metrics) watchOpened(ctx context.Context, cluster, kind string) {
	m.watches.Add(ctx, 1, metric.WithAttributes(
		attribute.String("drover.cluster", cluster),
		attribute.String("kind", kind)))
}

func (m *metrics) watchClosed(ctx context.Context, cluster, kind string) {
	m.watches.Add(ctx, -1, metric.WithAttributes(
		attribute.String("drover.cluster", cluster),
		attribute.String("kind", kind)))
}

// projectChanged records one project change that the project watch applies.
// cluster is the cluster of the project.
func (m *metrics) projectChanged(ctx context.Context, cluster string) {
	m.changed.Add(ctx, 1, metric.WithAttributes(attribute.String("drover.cluster", cluster)))
}

// syncError records one error that a reconcile run logs.
func (m *metrics) syncError(ctx context.Context) {
	m.errors.Add(ctx, 1)
}

// accountChanged records one write of the accounts. kind is the object kind,
// and action is create, update, move, or delete.
func (m *metrics) accountChanged(ctx context.Context, kind, action string) {
	m.accounts.Add(ctx, 1, metric.WithAttributes(
		attribute.String("kind", kind),
		attribute.String("action", action)))
}

// openbaoWritten records one OpenBao config write of cluster. outcome is ok,
// missing_mount, or error.
func (m *metrics) openbaoWritten(ctx context.Context, cluster, outcome string) {
	m.openbao.Add(ctx, 1, metric.WithAttributes(
		attribute.String("drover.cluster", cluster),
		attribute.String("outcome", outcome)))
}

// openbaoChanged records one write of the OpenBao state. kind is mount, role,
// or policy, and action is create, update, write, or delete.
func (m *metrics) openbaoChanged(ctx context.Context, kind, action string) {
	m.openbaoSet.Add(ctx, 1, metric.WithAttributes(
		attribute.String("kind", kind),
		attribute.String("action", action)))
}

// observeTrust reports the points of every project to the trust gauge, sorted
// by cluster and project. Past its cardinality limit, the SDK puts each new
// attribute set into one overflow point. So observeTrust reports the same
// projects below that limit at each collection.
func (m *metrics) observeTrust(_ context.Context, observer metric.Int64Observer) error {
	m.trustMu.Lock()
	defer m.trustMu.Unlock()
	refs := slices.SortedFunc(maps.Keys(m.trust), func(a, b projectRef) int {
		return cmp.Or(cmp.Compare(a.cluster, b.cluster), cmp.Compare(a.name, b.name))
	})
	for _, ref := range refs {
		for _, point := range m.trust[ref] {
			// Two invalid names with the same reason have the same attributes,
			// so the SDK keeps the last value.
			observer.Observe(point.value, metric.WithAttributeSet(point.attrs))
		}
	}
	return nil
}

// setTrust stores the points of the trust status of the project name of
// cluster. For a nil status, it removes the points. It does nothing when
// current returns false. It holds the lock of the points from the call of
// current to the store, so that the check is still true at the store.
func (m *metrics) setTrust(cluster, name string, status *trustStatus, current func() bool) {
	var points []trustPoint
	if status != nil {
		points = trustPoints(cluster, name, status)
	}
	ref := projectRef{cluster: cluster, name: name}
	m.trustMu.Lock()
	defer m.trustMu.Unlock()
	switch {
	case !current():
	case status == nil:
		delete(m.trust, ref)
	default:
		m.trust[ref] = points
	}
}

// forgetTrust removes the points of the project name of cluster.
func (m *metrics) forgetTrust(cluster, name string) {
	m.trustMu.Lock()
	defer m.trustMu.Unlock()
	delete(m.trust, projectRef{cluster: cluster, name: name})
}

// pruneTrust removes the points of each project for which drop returns true.
func (m *metrics) pruneTrust(drop func(projectRef) bool) {
	m.trustMu.Lock()
	defer m.trustMu.Unlock()
	for ref := range m.trust {
		if drop(ref) {
			delete(m.trust, ref)
		}
	}
}

// trustPoints returns one point per statement of status, or one point for a
// document error. It adds the attribute statement only for a valid name,
// because an invalid name is tenant text. It adds the attribute reason only
// for a statement that is not ready.
func trustPoints(cluster, name string, status *trustStatus) []trustPoint {
	point := func(ready bool, statement, reason string) trustPoint {
		attrs := []attribute.KeyValue{attribute.String("drover.cluster", cluster), attribute.String("project", name)}
		if statementName.MatchString(statement) {
			attrs = append(attrs, attribute.String("statement", statement))
		}
		if ready {
			return trustPoint{value: 1, attrs: attribute.NewSet(attrs...)}
		}
		return trustPoint{attrs: attribute.NewSet(append(attrs, attribute.String("reason", reason))...)}
	}
	if status.Error != nil {
		return []trustPoint{point(false, "", status.Error.Reason)}
	}
	if status.Statements == nil {
		return nil
	}
	out := make([]trustPoint, 0, len(*status.Statements))
	for _, item := range *status.Statements {
		out = append(out, point(item.Ready, item.Name, item.Reason))
	}
	return out
}

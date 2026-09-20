package filter

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestSpanName(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{
			name:   "a cluster-wide core collection",
			method: http.MethodGet,
			path:   "/k8s/clusters/c-m-x/api/v1/pods",
			want:   "GET /k8s/clusters/{cluster}/api/v1/pods",
		},
		{
			name:   "the namespace collection",
			method: http.MethodGet,
			path:   "/k8s/clusters/c-m-x/api/v1/namespaces",
			want:   "GET /k8s/clusters/{cluster}/api/v1/namespaces",
		},
		{
			name:   "one namespace",
			method: http.MethodGet,
			path:   "/k8s/clusters/c-m-x/api/v1/namespaces/luke",
			want:   "GET /k8s/clusters/{cluster}/api/v1/namespaces/{namespace}",
		},
		{
			name:   "a namespaced core collection",
			method: http.MethodGet,
			path:   "/k8s/clusters/c-m-x/api/v1/namespaces/luke/pods",
			want:   "GET /k8s/clusters/{cluster}/api/v1/namespaces/{namespace}/pods",
		},
		{
			name:   "a subresource of one object",
			method: http.MethodGet,
			path:   "/k8s/clusters/c-m-x/api/v1/namespaces/luke/pods/netshoot/log",
			want:   "GET /k8s/clusters/{cluster}/api/v1/namespaces/{namespace}/pods/{name}/log",
		},
		{
			name:   "a cluster-wide group collection",
			method: http.MethodGet,
			path:   "/k8s/clusters/c-m-x/apis/apps/v1/deployments",
			want:   "GET /k8s/clusters/{cluster}/apis/apps/v1/deployments",
		},
		{
			name:   "a namespaced group object",
			method: http.MethodGet,
			path:   "/k8s/clusters/c-m-x/apis/gateway.networking.k8s.io/v1/namespaces/luke/httproutes/web",
			want:   "GET /k8s/clusters/{cluster}/apis/gateway.networking.k8s.io/v1/namespaces/{namespace}/httproutes/{name}",
		},
		{
			name:   "the access review",
			method: http.MethodPost,
			path:   "/k8s/clusters/c-m-x/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
			want:   "POST /k8s/clusters/{cluster}/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
		},
		{
			name:   "core discovery",
			method: http.MethodGet,
			path:   "/k8s/clusters/c-m-x/api/v1",
			want:   "GET /k8s/clusters/{cluster}/api/v1",
		},
		{
			name:   "group discovery",
			method: http.MethodGet,
			path:   "/k8s/clusters/c-m-x/apis",
			want:   "GET /k8s/clusters/{cluster}/apis",
		},
		{
			name:   "a group version discovery",
			method: http.MethodGet,
			path:   "/k8s/clusters/c-m-x/apis/apps/v1",
			want:   "GET /k8s/clusters/{cluster}/apis/apps/v1",
		},
		{
			name:   "the Steve collection of the allowed set",
			method: http.MethodGet,
			path:   "/k8s/clusters/c-m-x/v1/namespaces",
			want:   "GET /k8s/clusters/{cluster}/v1/namespaces",
		},
		{
			name:   "the Rancher project list",
			method: http.MethodGet,
			path:   "/v3/projects",
			want:   "GET /v3/projects",
		},
		{
			name:   "a trailing slash",
			method: http.MethodGet,
			path:   "/k8s/clusters/c-m-x/api/v1/pods/",
			want:   "GET /k8s/clusters/{cluster}/api/v1/pods",
		},
		{
			name:   "the root",
			method: http.MethodGet,
			path:   "/",
			want:   "GET ",
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(test.method, test.path, nil)
			if got := spanName(req); got != test.want {
				t.Errorf("spanName = %q, want %q", got, test.want)
			}
		})
	}
}

func TestServerSpanCarriesTheRouteAttribute(t *testing.T) {
	h, exporter := newHarnessWithSpans(t, listUpstream(steveHandler("a"), namespaceListHandler))
	h.doDirect(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))

	span := requestSpan(t, exporter)
	want := "/k8s/clusters/{cluster}/api/v1/namespaces"
	if span.Name != http.MethodGet+" "+want {
		t.Errorf("span name = %q, want the method and the path template", span.Name)
	}
	route, ok := spanAttributeString(span, "http.route")
	if !ok {
		t.Fatal("the span has no http.route attribute")
	}
	if route != want {
		t.Errorf("http.route = %q, want %q", route, want)
	}
}

func TestHealthEndpointSpanIsNotRecorded(t *testing.T) {
	h, exporter := newHarnessWithSpans(t, listUpstream(steveHandler("a"), namespaceListHandler))
	h.doDirect(t, h.request(t, http.MethodGet, "/healthz", nil, nil))

	for _, span := range exporter.GetSpans() {
		if span.SpanKind == trace.SpanKindServer {
			t.Errorf("the health endpoint produced a server span %q", span.Name)
		}
	}
}

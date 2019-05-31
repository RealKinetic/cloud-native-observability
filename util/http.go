package util

import (
	"net/http"

	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
)

type contextMiddleware struct {
	handler http.Handler
}

// NewContextHandler returns an http.Handler which implements tracing and
// context middleware.
func NewContextHandler(handler http.Handler) http.Handler {
	// Add tracing middleware.
	handler = nethttp.Middleware(
		opentracing.GlobalTracer(),
		handler,
		nethttp.OperationNameFunc(func(r *http.Request) string {
			return r.Method + " " + r.URL.Path
		}),
	)
	return &contextMiddleware{handler}
}

func (c *contextMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Inject context with request data.
	ctx := contextWithRequest(r)
	r = r.WithContext(ctx)
	c.handler.ServeHTTP(w, r)
}

type instrumentedRoundTripper struct {
	tr http.RoundTripper
}

func (i *instrumentedRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	addContextHeaders(r)
	r, tracer := nethttp.TraceRequest(
		opentracing.GlobalTracer(),
		r,
		nethttp.OperationName(r.Method+" "+r.URL.Path),
	)
	defer tracer.Finish()
	return i.tr.RoundTrip(r)
}

// NewHTTPClient returns an http.Client that is instrumented for tracing and
// will propagate context values as request headers.
func NewHTTPClient() *http.Client {
	transport := &nethttp.Transport{}
	return &http.Client{Transport: &instrumentedRoundTripper{transport}}
}

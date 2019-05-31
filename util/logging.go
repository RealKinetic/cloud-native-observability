package util

import (
	"context"
	"net/http"
	"os"
	"reflect"

	"github.com/nats-io/nuid"
	"github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
)

type ctxKey int

const ctxValuesKey ctxKey = iota

const (
	requestIDHeader = "X-Ctx-RequestID"
)

type ctxValues struct {
	RequestID string
	Path      string
	Query     string
	Method    string
	IP        string
	Ref       string
}

func (c *ctxValues) addHeaders(r *http.Request) {
	// Propagate request id.
	if c.RequestID != "" {
		r.Header.Add(requestIDHeader, c.RequestID)
	}
}

func (c *ctxValues) fromRequest(r *http.Request) {
	id := r.Header.Get(requestIDHeader)
	if id != "" {
		c.RequestID = id
	}
}

// Init initializes logging and tracing for the given service. Call this before
// using logging or tracing.
func Init(serviceName string) error {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
	hook, err := newContextHook(serviceName)
	if err != nil {
		return err
	}
	log.AddHook(hook)

	tracer := initTracer(serviceName, log.StandardLogger())
	opentracing.InitGlobalTracer(tracer)
	return nil
}

type ctxHook struct {
	service  string
	hostname string
}

func newContextHook(serviceName string) (log.Hook, error) {
	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	return &ctxHook{
		service:  serviceName,
		hostname: host,
	}, nil
}

func (c *ctxHook) Levels() []log.Level {
	return []log.Level{
		log.PanicLevel,
		log.FatalLevel,
		log.ErrorLevel,
		log.WarnLevel,
		log.InfoLevel,
		log.TraceLevel,
	}
}

func (c *ctxHook) Fire(e *log.Entry) error {
	e.Data["service"] = c.service
	e.Data["host"] = c.hostname

	ctx := e.Context
	if ctx == nil {
		return nil
	}

	vals := ctx.Value(ctxValuesKey)
	if vals == nil {
		return nil
	}

	val := reflect.ValueOf(vals).Elem()
	context := make(map[string]interface{}, val.NumField())
	for i := 0; i < val.NumField(); i++ {
		valueField := val.Field(i)
		typeField := val.Type().Field(i)
		context[typeField.Name] = valueField.Interface()
	}
	e.Data["context"] = context
	return nil
}

func WithRef(ctx context.Context, ref string) context.Context {
	values := ctx.Value(ctxValuesKey)
	if values == nil {
		values = &ctxValues{}
		ctx = context.WithValue(ctx, ctxValuesKey, values)
	}
	values.(*ctxValues).Ref = ref
	return ctx
}

func contextWithRequest(r *http.Request) context.Context {
	values := &ctxValues{
		RequestID: nuid.Next(),
		Path:      r.URL.Path,
		Query:     r.URL.RawQuery,
		Method:    r.Method,
		IP:        r.RemoteAddr,
	}
	// Ensure we use propagated context headers.
	values.fromRequest(r)
	return context.WithValue(r.Context(), ctxValuesKey, values)
}

func addContextHeaders(r *http.Request) {
	ctx := r.Context()
	values := ctx.Value(ctxValuesKey)
	if values == nil {
		return
	}
	values.(*ctxValues).addHeaders(r)
}

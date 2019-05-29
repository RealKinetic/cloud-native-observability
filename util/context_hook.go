package util

import (
	"context"
	"net/http"
	"os"
	"reflect"

	log "github.com/sirupsen/logrus"
)

type ctxKey string

const ctxValuesKey = ctxKey("values")

type ctxValues struct {
	Path   string
	Query  string
	Method string
	IP     string
}

type ctxHook struct {
	service  string
	hostname string
}

func NewContextHook(serviceName string) (log.Hook, error) {
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
	val := reflect.ValueOf(vals)
	for i := 0; i < val.NumField(); i++ {
		valueField := val.Field(i)
		typeField := val.Type().Field(i)
		e.Data[typeField.Name] = valueField.Interface()
	}
	return nil
}

func ContextWithRequest(ctx context.Context, r *http.Request) context.Context {
	return context.WithValue(ctx, ctxValuesKey, ctxValues{
		Path:   r.URL.Path,
		Query:  r.URL.RawQuery,
		Method: r.Method,
		IP:     r.RemoteAddr,
	})
}

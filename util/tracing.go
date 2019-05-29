package util

import (
	"encoding/base64"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
	jaeger "github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/thrift"
)

// InitTracer returns an instance of Tracer that samples 100% of traces and
// logs all spans to stdout.
func InitTracer(service string, l *logrus.Logger) opentracing.Tracer {
	tracer, _ := jaeger.NewTracer(
		service,
		jaeger.NewConstSampler(true),
		newLogReporter(l),
	)
	return tracer
}

type logReporter struct {
	log        *logrus.Logger
	serializer *thrift.TSerializer
}

func newLogReporter(log *logrus.Logger) jaeger.Reporter {
	return &logReporter{log: log, serializer: thrift.NewTSerializer()}
}

func (l *logReporter) Report(span *jaeger.Span) {
	s := jaeger.BuildZipkinThrift(span)
	data, err := l.serializer.Write(s)
	if err != nil {
		panic(err)
	}
	encoded := base64.StdEncoding.EncodeToString(data)
	l.log.WithFields(logrus.Fields{
		"trace": encoded,
	}).Info("trace")
}

func (l *logReporter) Close() {}

package logrus_yc_hoook

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/yandex-cloud/go-genproto/yandex/cloud/logging/v1"
	ycsdk "github.com/yandex-cloud/go-sdk"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"math"
	"os"
	"time"
)

type Hook struct {
	sdk         *ycsdk.SDK
	config      Config
	entriesCh   chan *logrus.Entry
	entriesBuff []*logging.IncomingLogEntry
	ctx         context.Context
	ctxCancel   context.CancelFunc
}

var logLevelMap = map[logrus.Level]logging.LogLevel_Level{
	logrus.PanicLevel: logging.LogLevel_FATAL,
	logrus.FatalLevel: logging.LogLevel_FATAL,
	logrus.ErrorLevel: logging.LogLevel_ERROR,
	logrus.WarnLevel:  logging.LogLevel_WARN,
	logrus.InfoLevel:  logging.LogLevel_INFO,
	logrus.DebugLevel: logging.LogLevel_DEBUG,
	logrus.TraceLevel: logging.LogLevel_TRACE,
}

type Config struct {
	Credentials ycsdk.Credentials
	LogGroupId  string
	BufferSize  int
	SendTimeout time.Duration
}

func New(config Config) (*Hook, error) {
	ctx, cancel := context.WithCancel(context.Background())

	if config.BufferSize == 0 {
		config.BufferSize = 100
	}

	if config.SendTimeout == 0 {
		config.SendTimeout = 30 * time.Second
	}

	sdk, err := ycsdk.Build(ctx, ycsdk.Config{
		Credentials: config.Credentials,
	})

	if err != nil {
		cancel()

		return nil, err
	}

	hook := &Hook{
		sdk:         sdk,
		config:      config,
		entriesCh:   make(chan *logrus.Entry, config.BufferSize),
		entriesBuff: make([]*logging.IncomingLogEntry, 0, config.BufferSize),
		ctx:         ctx,
		ctxCancel:   cancel,
	}

	go hook.start()

	go func() {
		<-ctx.Done()

		close(hook.entriesCh)
	}()

	return hook, nil
}

func (h *Hook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
		logrus.TraceLevel,
	}
}

func (h *Hook) Fire(entry *logrus.Entry) error {
	select {
	case <-h.ctx.Done():
		return fmt.Errorf("failed to write log entry: yc hook closed")
	default:
		h.entriesCh <- entry
	}

	return nil
}

func (h *Hook) flushLogs() {
	if len(h.entriesBuff) > 0 {
		idx := int(math.Min(float64(h.config.BufferSize), float64(len(h.entriesBuff))))
		entriesToSend := h.entriesBuff[:idx]

		ctx, cancel := context.WithTimeout(h.ctx, h.config.SendTimeout)
		defer cancel()

		_, err := h.sdk.LogIngestion().LogIngestion().Write(ctx, &logging.WriteRequest{
			Entries: entriesToSend,
			Destination: &logging.Destination{
				Destination: &logging.Destination_LogGroupId{
					LogGroupId: h.config.LogGroupId,
				},
			},
		})

		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Error sending logs to YC: %s", err.Error())
		} else {
			h.entriesBuff = h.entriesBuff[idx:]
		}
	}
}

func (h *Hook) start() {
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-time.After(2 * time.Second):
			h.flushLogs()
		case rawEntry := <-h.entriesCh:
			jsonStruct, _ := structpb.NewStruct(rawEntry.Data)

			entry := &logging.IncomingLogEntry{
				Level:       logLevelMap[rawEntry.Level],
				Message:     rawEntry.Message,
				Timestamp:   timestamppb.New(rawEntry.Time),
				JsonPayload: jsonStruct,
			}

			h.entriesBuff = append(h.entriesBuff, entry)

			if len(h.entriesBuff) >= h.config.BufferSize {
				h.flushLogs()
			}
		}
	}
}

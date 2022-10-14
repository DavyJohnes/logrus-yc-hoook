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

const bufferSize = 100

type Hook struct {
	sdk         *ycsdk.SDK
	logGroupId  string
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

func New(credentials ycsdk.Credentials, logGroupId string) (*Hook, error) {
	sdk, err := ycsdk.Build(context.Background(), ycsdk.Config{
		Credentials: credentials,
	})

	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	hook := &Hook{
		sdk:         sdk,
		logGroupId:  logGroupId,
		entriesCh:   make(chan *logrus.Entry, bufferSize),
		entriesBuff: make([]*logging.IncomingLogEntry, 0, bufferSize),
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
		return fmt.Errorf("failed to write log entry: context canceled")
	default:
		h.entriesCh <- entry
	}

	return nil
}

func (h *Hook) flushLogs() {
	if len(h.entriesBuff) > 0 {
		idx := int(math.Min(float64(bufferSize), float64(len(h.entriesBuff))))
		entriesToSend := h.entriesBuff[:idx]

		_, err := h.sdk.LogIngestion().LogIngestion().Write(context.Background(), &logging.WriteRequest{
			Entries: entriesToSend,
			Destination: &logging.Destination{
				Destination: &logging.Destination_LogGroupId{
					LogGroupId: h.logGroupId,
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

			if len(h.entriesBuff) >= bufferSize {
				h.flushLogs()
			}
		}
	}
}

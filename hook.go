package logrus_yc_hoook

import (
	"context"
	"fmt"
	fifo "github.com/foize/go.fifo"
	"github.com/sirupsen/logrus"
	"github.com/yandex-cloud/go-genproto/yandex/cloud/logging/v1"
	ycsdk "github.com/yandex-cloud/go-sdk"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type Hook struct {
	sdk        *ycsdk.SDK
	q          *fifo.Queue
	logGroupId string
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

	hook := &Hook{
		sdk:        sdk,
		q:          fifo.NewQueue(),
		logGroupId: logGroupId,
	}

	go hook.start()

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
	h.q.Add(entry)

	return nil
}

func (h *Hook) start() {
	for {
		entries := make([]*logging.IncomingLogEntry, 0)

		for h.q.Len() > 0 {
			qItem := h.q.Next().(*logrus.Entry)

			jsonStruct, _ := structpb.NewStruct(qItem.Data)

			entry := &logging.IncomingLogEntry{
				Level:       logLevelMap[qItem.Level],
				Message:     qItem.Message,
				Timestamp:   timestamppb.New(qItem.Time),
				JsonPayload: jsonStruct,
			}

			entries = append(entries, entry)
		}

		if len(entries) > 0 {
			_, err := h.sdk.LogIngestion().LogIngestion().Write(context.Background(), &logging.WriteRequest{
				Entries: entries,
				Destination: &logging.Destination{
					Destination: &logging.Destination_LogGroupId{
						LogGroupId: h.logGroupId,
					},
				},
			})

			if err != nil {
				fmt.Printf("Error writing logs: %s", err.Error())
			}
		}

		time.Sleep(2 * time.Second)
	}
}

package logrus_yc_hoook

import (
	"context"
	"fmt"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/sirupsen/logrus"
	"github.com/yandex-cloud/go-genproto/yandex/cloud/logging/v1"
	ycsdk "github.com/yandex-cloud/go-sdk"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"os"
	"sync"
	"time"
)

type Hook struct {
	sdk              *ycsdk.SDK
	config           Config
	entriesCh        chan *logrus.Entry
	entriesBuff      []*logging.IncomingLogEntry
	entriesBuffMutex sync.Mutex
	wg               sync.WaitGroup
	ctx              context.Context
	ctxCancel        context.CancelFunc
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
	Credentials        ycsdk.Credentials
	LogGroupId         string
	BufferSize         int
	SendTimeout        time.Duration
	SendRetriesCount   uint
	SendRetriesTimeout time.Duration
}

func New(config Config) (*Hook, error) {
	var err error
	ctx, cancel := context.WithCancel(context.Background())

	defer func() {
		if err != nil {
			cancel()
		}
	}()

	if config.BufferSize == 0 {
		config.BufferSize = 100
	}

	if config.SendTimeout == 0 {
		config.SendTimeout = 30 * time.Second
	}

	if config.SendRetriesCount == 0 {
		config.SendRetriesCount = 2
	}

	if config.SendRetriesTimeout == 0 {
		config.SendRetriesTimeout = 2 * time.Second
	}

	sdk, err := ycsdk.Build(ctx, ycsdk.Config{
		Credentials: config.Credentials,
	})

	if err != nil {
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
	case h.entriesCh <- entry:
		// do nothing
	}

	return nil
}

func (h *Hook) Close() {
	go h.flushLogs()

	// wait unit all flushLogs will be finished
	h.wg.Wait()

	h.ctxCancel()
}

func (h *Hook) flushLogs() {
	h.wg.Add(1)
	defer h.wg.Done()

	h.entriesBuffMutex.Lock()
	defer h.entriesBuffMutex.Unlock()

	if len(h.entriesBuff) == 0 {
		return
	}

	entriesToSend := h.entriesBuff[:]

	if len(h.entriesBuff) > h.config.BufferSize {
		entriesToSend = h.entriesBuff[:h.config.BufferSize]
	}

	h.entriesBuff = h.entriesBuff[len(entriesToSend):]
	h.entriesBuffMutex.Unlock()

	ctx, cancel := context.WithTimeout(h.ctx, h.config.SendTimeout)
	defer cancel()

	_, err := h.sdk.LogIngestion().LogIngestion().Write(ctx, &logging.WriteRequest{
		Entries: entriesToSend,
		Destination: &logging.Destination{
			Destination: &logging.Destination_LogGroupId{
				LogGroupId: h.config.LogGroupId,
			},
		},
	}, grpc_retry.WithMax(h.config.SendRetriesCount), grpc_retry.WithPerRetryTimeout(h.config.SendRetriesTimeout))

	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error sending logs to YC: %s", err.Error())
	}
}

func (h *Hook) start() {
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-time.After(2 * time.Second):
			go h.flushLogs()
		case rawEntry := <-h.entriesCh:
			jsonStruct, _ := structpb.NewStruct(rawEntry.Data)

			entry := &logging.IncomingLogEntry{
				Level:       logLevelMap[rawEntry.Level],
				Message:     rawEntry.Message,
				Timestamp:   timestamppb.New(rawEntry.Time),
				JsonPayload: jsonStruct,
			}

			h.entriesBuffMutex.Lock()
			h.entriesBuff = append(h.entriesBuff, entry)
			h.entriesBuffMutex.Unlock()

			if len(h.entriesBuff) >= h.config.BufferSize {
				go h.flushLogs()
			}
		}
	}
}

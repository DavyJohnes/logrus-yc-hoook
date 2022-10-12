# Logrus Yandex Cloud Hook

[Logrus](https://github.com/sirupsen/logrus) hook for logging to [Yandex Cloud Logging](https://cloud.yandex.ru/docs/logging/).


## Usage example

```go
package helpers

import (
	ycsdk "github.com/yandex-cloud/go-sdk"
	"github.com/sirupsen/logrus"
	logrus_yc_hook "github.com/DavyJohnes/logrus-yc-hoook"
)

var Logger *logrus.Logger = createLogger()

func createLogger() *logrus.Logger {
	logger := logrus.New()

	if os.Getenv("DEBUG") == "1" {
		logger.SetLevel(logrus.DebugLevel)
	}

	ycLogGroupId := os.Getenv("YC_LOG_GROUP_ID")

	if ycLogGroupId != "" {
		var cred ycsdk.Credentials

		if token := os.Getenv("YC_TOKEN"); token != "" {
			cred = ycsdk.OAuthToken(token)
		} else {
			cred = ycsdk.InstanceServiceAccount()
		}

		ycHook, err := logrus_yc_hook.New(cred, ycLogGroupId)

		if err != nil {
			logger.Warnf("Unable to create yc logging hook: %s", err.Error())
		} else {
			logger.AddHook(ycHook)
		}
	}

	return logger
}

```

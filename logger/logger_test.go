package logger_test

import (
	"bytes"
	"encoding/json"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/relloyd/halfpipe/logger"
)

var _ = Describe("Logger", func() {
	logger := logger.NewLogger("test-service", "debug", true)
	It("Should have `test-service` as service name", func() {
		logOutput := bytes.NewBufferString("")
		logger.SetOutput(logOutput)

		logger.Info("Testing")
		var actual map[string]interface{}
		json.Unmarshal(logOutput.Bytes(), &actual)

		Expect(actual["service"]).To(Equal("test-service"))
	})

	It("Should have info as log leve", func() {
		var actual map[string]interface{}
		logOutput := bytes.NewBufferString("")
		logger.SetOutput(logOutput)

		logger.Info("Testing")
		json.Unmarshal(logOutput.Bytes(), &actual)

		Expect(actual["level"]).To(Equal("info"))
	})

	It("Should have warn as log level", func() {
		logOutput := bytes.NewBufferString("")
		logger.SetOutput(logOutput)

		logger.Warn("Testing")
		var actual map[string]interface{}
		json.Unmarshal(logOutput.Bytes(), &actual)

		Expect(actual["level"]).To(Equal("warning"))
	})

	It("Should have error as log level", func() {
		logOutput := bytes.NewBufferString("")
		logger.SetOutput(logOutput)

		logger.Error("Testing")
		var actual map[string]interface{}
		json.Unmarshal(logOutput.Bytes(), &actual)

		Expect(actual["level"]).To(Equal("error"))
		Expect(actual["stackTrace"]).ToNot(BeNil())
	})

	It("Should have `Testing` as msg", func() {
		logOutput := bytes.NewBufferString("")
		logger.SetOutput(logOutput)

		logger.Info("Testing")
		var actual map[string]interface{}
		json.Unmarshal(logOutput.Bytes(), &actual)

		Expect(actual["msg"]).To(Equal("Testing"))
	})
})

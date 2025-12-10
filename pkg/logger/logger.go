package logger

import (
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var base *zap.Logger

// Init sets up zap logger with a rotating-friendly file sink.
func Init(level string, file string) (*zap.Logger, error) {
	lvl := zap.InfoLevel
	if err := lvl.Set(strings.ToLower(level)); err != nil {
		lvl = zap.InfoLevel
	}

	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "ts"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	// Ensure log directory exists.
	if file != "" {
		if err := os.MkdirAll(dir(file), 0o755); err != nil {
			return nil, err
		}
	}

	rotator := &lumberjack.Logger{
		Filename:   file,
		MaxSize:    10, // megabytes
		MaxBackups: 5,
		MaxAge:     30,   // days
		Compress:   true, // compress old logs
	}
	fileSink := zapcore.AddSync(rotator)
	consoleSink := zapcore.AddSync(os.Stdout)
	core := zapcore.NewTee(
		zapcore.NewCore(zapcore.NewJSONEncoder(encoderCfg), fileSink, lvl),
		zapcore.NewCore(zapcore.NewConsoleEncoder(encoderCfg), consoleSink, lvl),
	)
	base = zap.New(core, zap.AddCaller(), zap.ErrorOutput(zapcore.Lock(os.Stderr)))
	return base, nil
}

func L() *zap.Logger {
	if base == nil {
		logger, _ := zap.NewProduction()
		return logger
	}
	return base
}

func dir(path string) string {
	idx := strings.LastIndex(path, "/")
	if idx == -1 {
		return "."
	}
	return path[:idx]
}

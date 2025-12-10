package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"uveitis/backend/pkg/config"
	"uveitis/backend/pkg/logger"
	"uveitis/backend/pkg/server"
	"uveitis/backend/pkg/storage"

	"go.uber.org/zap"
)

func main() {
	configPath := flag.String("config", "./config.yaml", "配置文件路径")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("读取配置失败: %v", err)
	}

	logg, err := logger.Init(cfg.Logging.Level, cfg.Logging.File)
	if err != nil {
		log.Fatalf("初始化日志失败: %v", err)
	}
	defer logg.Sync()

	if err := os.MkdirAll(filepath.Dir(cfg.Database.Path), 0o755); err != nil {
		log.Fatalf("创建数据目录失败: %v", err)
	}

	store, err := storage.Open(cfg.Database.Path, logg)
	if err != nil {
		log.Fatalf("连接数据库失败: %v", err)
	}
	defer store.Close()

	s := server.New(cfg, logg, store)
	router := s.Router()
	addr := fmt.Sprintf("%s:%d", cfg.App.Host, cfg.App.Port)
	go func() {
		logg.Info("server started", zap.String("addr", addr))
		if err := router.Run(addr); err != nil {
			logg.Fatal("server exit", zap.Error(err))
		}
	}()

	// Graceful shutdown on Ctrl+C (for desktop use).
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logg.Info("shutdown")
}

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"jiaxinbinggan/internal/config"
	"jiaxinbinggan/internal/migrate"
	"jiaxinbinggan/internal/view"
)

func main() {
	configPath := flag.String("config", "configs/example.yaml", "配置文件路径")
	debug := flag.Bool("debug", false, "在 TUI 中显示 MySQL 查询和 PostgreSQL 操作")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取配置失败: %v\n", err)
		os.Exit(1)
	}
	if *debug {
		cfg.Job.Debug = true
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	events := make(chan view.Event, 1024)
	viewDone := make(chan struct{})
	if cfg.Job.View.Enabled {
		go func() {
			defer close(viewDone)
			refresh := time.Duration(cfg.Job.View.RefreshIntervalMs) * time.Millisecond
			view.NewTUI(refresh).Run(ctx, events)
		}()
	} else {
		close(viewDone)
	}

	runner := migrate.NewRunner(cfg, events)
	err = runner.Run(ctx)
	close(events)
	<-viewDone
	if err != nil {
		fmt.Fprintf(os.Stderr, "导入失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("导入完成")
}

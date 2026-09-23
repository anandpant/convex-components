// cliproxy-capture-exporter: private durable admission and independently scoped delivery.
package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/anandpant/convex-components/cliproxy-capture/internal/capture"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	socket := flag.String("socket", "", "private Unix socket path")
	db := flag.String("db", "", "private outbox path")
	budget := flag.Int64("budget-bytes", 2<<30, "outbox database ceiling")
	reserve := flag.Uint64("reserve-bytes", 2<<30, "minimum filesystem free space")
	delivery := flag.String("delivery-config", "", "private destination credential bundle; empty disables remote delivery")
	flag.Parse()
	if err := run(*socket, *db, *budget, *reserve, *delivery); err != nil {
		fmt.Fprintln(os.Stderr, "capture exporter stopped:", err)
		os.Exit(1)
	}
}
func run(socket, db string, budget int64, reserve uint64, deliveryPath string) error {
	var delivery capture.DeliveryConfig
	if deliveryPath != "" {
		var err error
		delivery, err = capture.LoadDeliveryConfig(deliveryPath)
		if err != nil {
			return err
		}
	}
	if !filepath.IsAbs(socket) {
		return fmt.Errorf("absolute socket required")
	}
	dir := filepath.Dir(socket)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	s, err := os.Stat(dir)
	if err != nil || s.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("socket directory must be private")
	}
	lock, err := os.OpenFile(socket+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return fmt.Errorf("exporter already running")
	}
	if s, err := os.Lstat(socket); err == nil {
		if s.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("refusing non-socket replacement")
		}
		if err = os.Remove(socket); err != nil {
			return err
		}
	}
	outbox, err := capture.OpenOutbox(db, budget, reserve)
	if err != nil {
		return err
	}
	defer outbox.Close()
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return err
	}
	defer listener.Close()
	if err = os.Chmod(socket, 0600); err != nil {
		return err
	}
	server := &http.Server{Handler: outbox, ReadHeaderTimeout: time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, MaxHeaderBytes: 8192}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	delivered := outbox.RunDelivery(ctx, delivery, nil)
	defer func() { stop(); <-delivered }()
	go func() {
		<-ctx.Done()
		deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(deadline)
	}()
	err = server.Serve(listener)
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/codingsandmore/rayscan/config"
	"github.com/codingsandmore/rayscan/connection"
	"github.com/codingsandmore/rayscan/onchain"
	log "github.com/sirupsen/logrus"
)

func main() {
	cfg, err := config.LoadConfig(config.DefaultConfigPath)
	if err != nil {
		log.Errorf("Error loading config: %s\n", err)
		os.Exit(1)
	}

	rpcPool, err := connection.NewRPCClientPool(cfg.Nodes)
	if err != nil {
		log.Errorf("Error creating rpc pool: %s\n", err)
		os.Exit(1)
	}
	defer rpcPool.Close()

	pairCollector := onchain.NewPairCollector()
	pairCollector.Start(nil)

	txAnalyzer := onchain.NewTxAnalyzer(rpcPool)
	txAnalyzer.Start(pairCollector.Channel())

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var observers []*onchain.LogObserver
	for _, v := range rpcPool.Connections {
		if !v.ConnectionInfo.Observer {
			continue
		}

		obs := onchain.NewLogObserver(rpcPool, v.ConnectionInfo.Name)
		if err := obs.Start(ctx, txAnalyzer.Channel()); err != nil {
			log.Errorf("Error starting %s log observer: %s\n", v.ConnectionInfo.Name, err)
			os.Exit(1)
		}

		observers = append(observers, obs)
	}

	var stopChan = make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	<-stopChan // wait for SIGINT

	log.Warnf("Interrupted; stopping...\n")
	ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	for _, obs := range observers {
		if err := obs.Stop(ctx); err != nil {
			log.Errorf("Error stopping %s log observer: %s\n", obs.ConnectionName(), err)
		}
	}

	if err := txAnalyzer.Stop(ctx); err != nil {
		log.Errorf("Error stopping tx analyzer: %s\n", err)
	}

	if err := pairCollector.Stop(ctx); err != nil {
		log.Errorf("Error stopping pair collector: %s\n", err)
	}
}

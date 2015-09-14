package main

import "fmt"
import "time"
import "os"
import "os/signal"
import log "github.com/Sirupsen/logrus"

import (
	"mysqlCollector"
	"mm"
)

func main() {
	log.SetOutput(os.Stderr)
	log.SetLevel(log.DebugLevel)

	fmt.Println("Collector starts")
	mc := mysqlCollector.NewMysqlCollector("root@tcp(localhost:3306)/test")
	clock := time.NewTicker(time.Second * 1)
	collectionChan := make(chan *mm.Collection)
	mc.Start(clock.C, collectionChan)

	spool := &mm.DataStorage{}
	ag := mm.NewAggregator(collectionChan, *spool)
	ag.Start()

signalChan := make(chan os.Signal, 1)
cleanupDone := make(chan bool)
signal.Notify(signalChan, os.Interrupt)
go func() {
    for _ = range signalChan {
        fmt.Println("\nReceived an interrupt, stopping services...\n")
        cleanupDone <- true
    }
}()
<-cleanupDone

}

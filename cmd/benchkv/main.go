// INTERLOCKyright 2020 WHTCORPS INC, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a INTERLOCKy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	_ "github.com/go-allegrosql-driver/allegrosql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/whtcorpsinc/berolinaAllegroSQL/terror"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/log"
	"github.com/whtcorpsinc/milevadb/causetstore/einsteindb"
	"github.com/whtcorpsinc/milevadb/ekv"
	"go.uber.org/zap"
)

var (
	causetstore ekv.CausetStorage
	dataCnt     = flag.Int("N", 1000000, "data num")
	workerCnt   = flag.Int("C", 400, "concurrent num")
	FIDelAddr   = flag.String("fidel", "localhost:2379", "fidel address:localhost:2379")
	valueSize   = flag.Int("V", 5, "value size in byte")

	txnCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "einsteindb",
			Subsystem: "txn",
			Name:      "total",
			Help:      "Counter of txns.",
		}, []string{"type"})

	txnRolledbackCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "einsteindb",
			Subsystem: "txn",
			Name:      "failed_total",
			Help:      "Counter of rolled back txns.",
		}, []string{"type"})

	txnDurations = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "einsteindb",
			Subsystem: "txn",
			Name:      "durations_histogram_seconds",
			Help:      "Txn latency distributions.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"type"})
)

// Init initializes information.
func Init() {
	driver := einsteindb.Driver{}
	var err error
	causetstore, err = driver.Open(fmt.Sprintf("einsteindb://%s?cluster=1", *FIDelAddr))
	terror.MustNil(err)

	prometheus.MustRegister(txnCounter)
	prometheus.MustRegister(txnRolledbackCounter)
	prometheus.MustRegister(txnDurations)
	http.Handle("/metrics", promhttp.Handler())

	go func() {
		err1 := http.ListenAndServe(":9191", nil)
		terror.Log(errors.Trace(err1))
	}()
}

// batchRW makes sure conflict free.
func batchRW(value []byte) {
	wg := sync.WaitGroup{}
	base := *dataCnt / *workerCnt
	wg.Add(*workerCnt)
	for i := 0; i < *workerCnt; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < base; j++ {
				txnCounter.WithLabelValues("txn").Inc()
				start := time.Now()
				k := base*i + j
				txn, err := causetstore.Begin()
				if err != nil {
					log.Fatal(err.Error())
				}
				key := fmt.Sprintf("key_%d", k)
				err = txn.Set([]byte(key), value)
				terror.Log(errors.Trace(err))
				err = txn.Commit(context.Background())
				if err != nil {
					txnRolledbackCounter.WithLabelValues("txn").Inc()
					terror.Call(txn.Rollback)
				}

				txnDurations.WithLabelValues("txn").Observe(time.Since(start).Seconds())
			}
		}(i)
	}
	wg.Wait()
}

func main() {
	flag.Parse()
	log.SetLevel(zap.ErrorLevel)
	Init()

	value := make([]byte, *valueSize)
	t := time.Now()
	batchRW(value)
	resp, err := http.Get("http://localhost:9191/metrics")
	terror.MustNil(err)

	defer terror.Call(resp.Body.Close)
	text, err1 := ioutil.ReadAll(resp.Body)
	terror.Log(errors.Trace(err1))

	fmt.Println(string(text))

	fmt.Printf("\nelapse:%v, total %v\n", time.Since(t), *dataCnt)
}

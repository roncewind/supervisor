package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"runtime/metrics"
	"time"
)

var workQueue chan int
var workerTimeout float64 = 8

// ----------------------------------------------------------------------------
// simulated worker struct
type simulatedWorker struct {
	ctx      context.Context
	err      error
	id       string
	shutdown bool
}

func NewSimulatedWorker(ctx context.Context, id string) Worker {
	return &simulatedWorker{
		ctx:      ctx,
		id:       id,
		shutdown: false,
	}
}

func (worker *simulatedWorker) getContext() context.Context {
	return worker.ctx
}

func (worker *simulatedWorker) getError() error {
	return worker.err
}

func (worker *simulatedWorker) setError(err error) {
	worker.err = err
}

func (worker *simulatedWorker) getId() string {
	return worker.id
}

func (worker *simulatedWorker) getShutdown() bool {
	return worker.shutdown
}

func (worker *simulatedWorker) setShutdown(shutdown bool) {
	worker.shutdown = shutdown
}

// ----------------------------------------------------------------------------
// this function simulates do work as a worker
// PONDER:  private function, should only be called from Start?
func (worker *simulatedWorker) doWork() (err error) {

	// Worker simulation
	for {
		// use select to test if our context has completed
		select {
		case <-worker.ctx.Done():
			worker.shutdown = true
			shutdown(worker.getId())
			return nil
		case myWork := <-workQueue:
			// Now do whatever work we should do.
			t := time.Now()
			fmt.Println(worker.id, "has", myWork, "work to do")
			//simulate doing some work... for "myWork" number of seconds
			time.Sleep(time.Duration(myWork) * time.Second)
			q := rand.Intn(100)
			// fmt.Println(worker.id, "q:", q, "since:", time.Since(t).Seconds(), "workerTimeout:", workerTimeout)
			if q < 10 {
				// failed work unit
				// re-queue the work unit for re-processing.
				// this blocks if the work queue is full.
				workQueue <- myWork
				// simulate 10% chance of panic
				panic(fmt.Sprintf("with %d", q))
			} else if q < 20 {
				// failed work unit
				// re-queue the work unit for re-processing.
				// this blocks if the work queue is full.
				workQueue <- myWork
				// simulate 10% chance of failure
				return fmt.Errorf("error on %d", q)
			} else if since := time.Since(t).Seconds(); since > workerTimeout {
				fmt.Println(worker.id, "timeout:", since, ">", workerTimeout)
				// simulate timeout extension
				workerTimeout = workerTimeout + 2
				fmt.Println(worker.id, "workerTimeout extended to ", workerTimeout)
				// PONDER:  It could be that our worker needs to be "richer"
				// and simulate a cumulative amount of work time and continue
				// to work until some "hard stop" amount of time.  It might be
				// that we need to signal a queue to extend our time, but
				// keep working.  all that sort of logic would need to go here.

				// fail the work unit
				// re-queue the work unit for re-processing.
				// this blocks if the work queue is full.
				workQueue <- myWork
				// PONDER:  this will lead to a subtle error when shutting down
				// the work unit will be lost, but since this is a simulated
				// worker, there's no problem.

				// if the work has taken more than allow timeout, return a timeout error
				return errors.New("timeout")
			} else {
				fmt.Printf("%v completed with %d.\n", worker.id, q)
				emitRuntimeStats("Work complete")
			}
		}
	}
}

// ----------------------------------------------------------------------------
func shutdown(id string) {
	// simulate handling the context being cancelled
	now := time.Now()
	fmt.Printf("%v cancelled\n", id)
	time.Sleep(time.Duration(rand.Intn(5)) * time.Second)
	fmt.Printf("%v shutdown with cancel, after %.1f.\n", id, time.Since(now).Seconds())
}

// ----------------------------------------------------------------------------
func main() {
	// number of desired workers
	numWorkers := 10

	//create a few "work items" and put in the queue
	workQueue = make(chan int, numWorkers)
	go func() {
		for i := 0; i < 10; i++ {
			workQueue <- rand.Intn(15)
			time.Sleep(time.Duration(rand.Intn(5)) * time.Second)
		}
		fmt.Println("**** all jobs submitted ****")
	}()

	<-StartSupervisor(NewSimulatedWorker, numWorkers, 60)
	emitRuntimeStats("End main")
	emitMetrics()
}

// ----------------------------------------------------------------------------
func emitRuntimeStats(label string) {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	fmt.Println("=====================================")
	fmt.Println("===", label)
	fmt.Println("= Num Gorouting:", runtime.NumGoroutine())
	fmt.Println("= alloc_bytes:", stats.Alloc)
	fmt.Println("= sys_bytes:", stats.Sys)
	fmt.Println("= malloc_count:", stats.Mallocs)
	fmt.Println("= free_count:", stats.Frees)
	fmt.Println("= heap_objects:", stats.HeapObjects)
	fmt.Println("= total_gc_pause_ns:", stats.PauseTotalNs)
	fmt.Println("= total_gc_runs:", stats.NumGC)
	fmt.Println("=====================================")
}

func emitMetrics() {
	// Get descriptions for all supported metrics.
	descs := metrics.All()

	// Create a sample for each metric.
	samples := make([]metrics.Sample, len(descs))
	for i := range samples {
		samples[i].Name = descs[i].Name
	}

	// Sample the metrics. Re-use the samples slice if you can!
	metrics.Read(samples)

	// Iterate over all results.
	for _, sample := range samples {
		// Pull out the name and value.
		name, value := sample.Name, sample.Value

		// Handle each sample.
		switch value.Kind() {
		case metrics.KindUint64:
			fmt.Printf("%s: %d\n", name, value.Uint64())
		case metrics.KindFloat64:
			fmt.Printf("%s: %f\n", name, value.Float64())
		case metrics.KindFloat64Histogram:
			// The histogram may be quite large, so let's just pull out
			// a crude estimate for the median for the sake of this example.
			fmt.Printf("%s: %f\n", name, medianBucket(value.Float64Histogram()))
		case metrics.KindBad:
			// This should never happen because all metrics are supported
			// by construction.
			panic("bug in runtime/metrics package!")
		default:
			// This may happen as new metrics get added.
			//
			// The safest thing to do here is to simply log it somewhere
			// as something to look into, but ignore it for now.
			// In the worst case, you might temporarily miss out on a new metric.
			fmt.Printf("%s: unexpected metric Kind: %v\n", name, value.Kind())
		}
	}
}

func medianBucket(h *metrics.Float64Histogram) float64 {
	total := uint64(0)
	for _, count := range h.Counts {
		total += count
	}
	thresh := total / 2
	total = 0
	for i, count := range h.Counts {
		total += count
		if total >= thresh {
			return h.Buckets[i]
		}
	}
	panic("should not happen")
}

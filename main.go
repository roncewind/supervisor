package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
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
			fmt.Println("Worker", worker.id, "has", myWork, "work to do")
			//simulate doing some work... max of 10 seconds
			time.Sleep(time.Duration(myWork) * time.Second)
			q := rand.Intn(100)
			if q < 10 {
				// failed work unit, NACK simulation
				workQueue <- myWork
				// simulate 10% chance of panic
				panic(fmt.Sprintf("with %d", q))
			} else if q < 20 {
				// failed work unit
				workQueue <- myWork
				// simulate 10% chance of failure
				return fmt.Errorf("error on %d", q)
			} else if since := time.Since(t).Seconds(); since > workerTimeout {
				// failed work unit
				workQueue <- myWork
				fmt.Println("Worker", worker.id, "timeout:", since, ">", workerTimeout)
				// simulate timeout extension
				workerTimeout = workerTimeout + 2
				// if the work has taken more than allow timeout, return a timeout error
				return errors.New("timeout")
			} else {
				fmt.Printf("Worker %v completed with %d.\n", worker.id, q)
			}
		}
	}
}

// ----------------------------------------------------------------------------
func shutdown(id string) {
	// simulate handling the context being cancelled
	now := time.Now()
	fmt.Printf("Worker %s cancelled\n", id)
	time.Sleep(time.Duration(rand.Intn(5)) * time.Second)
	fmt.Printf("Worker %s shutdown with cancel, after %.1f.\n", id, time.Since(now).Seconds())
}

// ----------------------------------------------------------------------------
func main() {
	// number of desired workers
	numWorkers := 10

	//create a few "work items" and put in the queue
	workQueue = make(chan int)
	go func() {
		for i := 0; i < 10; i++ {
			workQueue <- rand.Intn(10)
			time.Sleep(time.Duration(rand.Intn(5)) * time.Second)
		}
		fmt.Println("**** all jobs submitted ****")
	}()

	StartSupervisor(NewSimulatedWorker, numWorkers)
}

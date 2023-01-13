package main

import (
	"errors"
	"fmt"
	"math/rand"
	"time"
)

var workQueue chan int
var workerTimeout float64 = 8

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

	StartSupervisor(numWorkers)
}

func shutdown(id int) {
	// simulate handling the context being cancelled
	now := time.Now()
	fmt.Printf("Worker %d cancelled\n", id)
	time.Sleep(time.Duration(rand.Intn(5)) * time.Second)
	fmt.Printf("Worker %d shutdown with cancel, after %.1f.\n", id, time.Since(now).Seconds())
}

// ----------------------------------------------------------------------------
// this function simulates do work as a worker
// PONDER:  private function, should only be called from Start?
func (worker *Worker) doWork() (err error) {

	// Worker simulation
	for {
		// use select to test if our context has completed
		select {
		case <-worker.ctx.Done():
			worker.shutdown = true
			shutdown(worker.id)
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
				fmt.Printf("Worker %d completed with %d.\n", worker.id, q)
			}
		}
	}
}

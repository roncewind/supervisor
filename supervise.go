package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// number of desired workers
const numWorkers = 10

func main() {
	// make a buffered channel with the space for all workers
	//  workers will signal on this channel if they die
	workerChan := make(chan *Worker, numWorkers)
	// PONDER:  close the workerChan here or in the goroutine?
	//  probably doesn't matter in this case, but something to keep an eye on.
	defer close(workerChan)

	ctx, cancel := context.WithCancel(context.Background())

	// start up a number of workers.
	for i := 0; i < numWorkers; i++ {
		i := i
		worker := &Worker{
			ctx: ctx,
			id:  i,
		}
		go worker.Start(workerChan)
	}

	// Monitor a chan and start a new worker if one has stopped:
	//   - read the channel
	//	 - block until something is written
	//   - check if worker is shutting down
	//	 	- if not, re-start the worker
	go func() {
		shutdownCount := numWorkers
		for worker := range workerChan {

			if worker.shutdown {
				shutdownCount--
			} else {
				// log the error
				fmt.Printf("Worker %d stopped with err: %s\n", worker.id, worker.err)
				// reset err
				worker.err = nil

				// a goroutine has ended, restart it
				go worker.Start(workerChan)
				fmt.Printf("Worker %d restarted\n", worker.id)
			}

			if shutdownCount == 0 {
				fmt.Println("All workers shutdown, exiting")
				os.Exit(0)
			}
		}
	}()

	// when shutdown signalled, wait for 15 seconds for graceful shutdown
	//	 to complete, then force
	wait := gracefulShutdown(cancel, 15*time.Second)
	<-wait
}

// ----------------------------------------------------------------------------
// gracefulShutdown waits for terminating syscalls then signals workers to shutdown
func gracefulShutdown(cancel func(), timeout time.Duration) <-chan struct{} {
	wait := make(chan struct{})

	go func() {
		defer close(wait)
		sig := make(chan os.Signal, 1)
		defer close(sig)

		// PONDER: add any other syscalls?
		// SIGHUP - hang up, lost controlling terminal
		// SIGINT - interrupt (ctrl-c)
		// SIGQUIT - quit (ctrl-\)
		// SIGTERM - request to terminate
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)
		killsig := <-sig
		switch killsig {
		case syscall.SIGINT:
			fmt.Println("Killed with ctrl-c")
		case syscall.SIGTERM:
			fmt.Println("Killed with request to terminate")
		case syscall.SIGQUIT:
			fmt.Println("Killed with ctrl-\\")
		case syscall.SIGHUP:
			fmt.Println("Killed with hang up")
		}

		timeoutSignal := make(chan struct{})
		defer close(timeoutSignal)
		// set timeout for the cleanup to be done to prevent system hang
		timeoutFunc := time.AfterFunc(timeout, func() {
			fmt.Printf("Timeout %.1fs have elapsed, force exit\n", timeout.Seconds())
			timeoutSignal <- struct{}{}
		})

		defer timeoutFunc.Stop()

		// cancel the context
		cancel()
		fmt.Println("Shutdown signalled.")

		// wait for timeout to finish and exit
		<-timeoutSignal
		os.Exit(0)
	}()

	return wait
}

// ----------------------------------------------------------------------------
// simulated worker struct
type Worker struct {
	ctx      context.Context
	err      error
	id       int
	shutdown bool
}

// ----------------------------------------------------------------------------
// this function can start a new worker and re-start a worker if it's failed
func (worker *Worker) Start(workerChan chan<- *Worker) (err error) {
	// make the goroutine signal its death, whether it's a panic or a return
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				worker.err = err
			} else {
				worker.err = fmt.Errorf("panic happened %v", r)
			}
		} else {
			worker.err = err
		}
		workerChan <- worker
	}()
	worker.shutdown = false
	return worker.doWork()
}

// ----------------------------------------------------------------------------
// this function simulates do work as a worker
// PONDER:  private function, should only be called from Start?
func (worker *Worker) doWork() (err error) {
	// Worker simulation
	for {
		select {
		case <-worker.ctx.Done():
			worker.shutdown = true
			// simulate handling the context being cancelled
			now := time.Now()
			fmt.Printf("Worker %d cancelled\n", worker.id)
			time.Sleep(time.Duration(rand.Intn(5)) * time.Second)
			fmt.Printf("Worker %d shutdown with cancel, after %.1f.\n", worker.id, time.Since(now).Seconds())
			return nil
		default:
			t := time.Now()
			//simulate doing some work... max of 10 seconds
			time.Sleep(time.Duration(rand.Intn(10)) * time.Second)
			q := rand.Intn(100)
			if q < 10 {
				// simulate 10% chance of panic
				panic(fmt.Sprintf("with %d", q))
			} else if q < 20 {
				// simulate 10% chance of failure
				return fmt.Errorf("error on %d", q)
			} else if time.Since(t).Seconds() > 8 {
				// simulate timeout
				// if the work has taken more than 8 seconds, timeout
				return errors.New("timeout")
			} else {
				fmt.Printf("Worker %d completed with %d.\n", worker.id, q)
			}
		}
	}
}

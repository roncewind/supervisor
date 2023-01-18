package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// ----------------------------------------------------------------------------
type Worker interface {
	getContext() context.Context
	getId() string
	setError(error)
	getError() error
	setShutdown(bool)
	getShutdown() bool
	doWork() error
}

// ----------------------------------------------------------------------------
func StartSupervisor(newWorker func(context.Context, string) Worker, numWorkers int) {
	// make a buffered channel with the space for all workers
	//  workers will signal on this channel if they die
	workerChan := make(chan *Worker, numWorkers)
	// PONDER:  close the workerChan here or in the goroutine?
	//  probably doesn't matter in this case, but something to keep an eye on.
	defer close(workerChan)

	ctx, cancel := context.WithCancel(context.Background())

	// start up a number of workers.
	for i := 0; i < numWorkers; i++ {
		id := fmt.Sprintf("worker-%d", i)
		worker := newWorker(ctx, id)
		go Start(worker, workerChan)
	}

	// when shutdown signalled, wait for 15 seconds for graceful shutdown
	//	 to complete, then force
	sigShutdown := gracefulShutdown(cancel, 15*time.Second)

	// Monitor a chan and start a new worker if one has stopped:
	//   - read the channel
	//	 - block until something is written
	//   - check if worker is shutting down
	//	 	- if not, re-start the worker
	go func() {
		shutdownCount := numWorkers
		for worker := range workerChan {

			if (*worker).getShutdown() {
				shutdownCount--
			} else {
				// log the error
				fmt.Printf("Worker %v stopped with err: %s\n", (*worker).getId(), (*worker).getError())
				// reset err
				(*worker).setError(nil)

				// a goroutine has ended, restart it
				go Start(*worker, workerChan)
				fmt.Printf("Worker %s restarted\n", (*worker).getId())
			}

			if shutdownCount == 0 {
				fmt.Println("All workers shutdown, exiting")
				sigShutdown <- struct{}{}
			}
		}
	}()

	<-sigShutdown
}

// ----------------------------------------------------------------------------
// gracefulShutdown waits for terminating syscalls then signals workers to shutdown
func gracefulShutdown(cancel func(), timeout time.Duration) chan struct{} {
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

		// set timeout for the cleanup to be done to prevent system hang
		timeoutSignal := make(chan struct{})
		timeoutFunc := time.AfterFunc(timeout, func() {
			fmt.Printf("Timeout %.1fs have elapsed, force exit\n", timeout.Seconds())
			close(timeoutSignal)
		})

		defer timeoutFunc.Stop()

		// cancel the context
		cancel()
		fmt.Println("Shutdown signalled.")

		// wait for timeout to finish and exit
		<-timeoutSignal
		wait <- struct{}{}
	}()

	return wait
}

// ----------------------------------------------------------------------------
// this function can start a new worker and re-start a worker if it's failed
func Start(worker Worker, workerChan chan<- *Worker) (err error) {
	// make the goroutine signal its death, whether it's a panic or a return
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				worker.setError(err)
			} else {
				worker.setError(fmt.Errorf("panic happened %v", r))
			}
		} else {
			// a little tricky go code here.
			//  err is picked up from the doWork return
			worker.setError(err)
		}
		workerChan <- &worker
	}()
	worker.setShutdown(false)
	return worker.doWork()
}

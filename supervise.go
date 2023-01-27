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
	doWork() error
	getContext() context.Context
	getId() string
	getError() error
	setError(error)
	getShutdown() bool
	setShutdown(bool)
}

// type newWorker func(ctx context.Context, id string)
// type Supervisor struct {
// 	newWorker
// 	numWorkers      int
// 	shutdownTimeout time.Duration
// }

// ----------------------------------------------------------------------------
func StartSupervisor(newWorker func(ctx context.Context, id string) Worker, numWorkers int, shutdownTimeout time.Duration) {
	// make a buffered channel with the space for all workers
	//  workers will signal on this channel if they die
	workerChan := make(chan *Worker, numWorkers)

	ctx, cancel := context.WithCancel(context.Background())

	// start up a number of workers.
	for i := 0; i < numWorkers; i++ {
		id := fmt.Sprintf("worker-%d", i)
		worker := newWorker(ctx, id)
		go Start(worker, workerChan)
	}

	// when shutdown signalled, wait for `shutdownTimeout` seconds for graceful shutdown
	//	 to complete, then force
	sigShutdown := gracefulShutdown(cancel, shutdownTimeout*time.Second)

	// Monitor a chan and start a new worker if one has stopped:
	//   - read the channel
	//	 - block until something is written
	//   - check if worker is shutting down
	//	 	- if not, re-start the worker
	go func() {
		shutdownCount := numWorkers
		for worker := range workerChan {
			if (*worker).getShutdown() {
				fmt.Printf("%v is shutdown\n", (*worker).getId())
				shutdownCount--
			} else {
				// log the error
				fmt.Printf("%v stopped with err: %s\n", (*worker).getId(), (*worker).getError())
				// reset err
				(*worker).setError(nil)

				// a goroutine has ended, restart it
				go Start(*worker, workerChan)
				fmt.Printf("%v restarted\n", (*worker).getId())
			}

			if shutdownCount == 0 {
				fmt.Println("All workers shutdown, exiting")
				close(workerChan)
				sigShutdown <- struct{}{}
			}
		}
	}()

	<-sigShutdown
}

// ----------------------------------------------------------------------------
// gracefulShutdown waits for terminating syscalls then signals workers to shutdown
func gracefulShutdown(cancel func(), timeout time.Duration) chan struct{} {
	sigShutdown := make(chan struct{})

	go func() {
		defer close(sigShutdown)
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
		sigShutdown <- struct{}{}
	}()

	return sigShutdown
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

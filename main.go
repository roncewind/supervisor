package main

import (
	"errors"
	"fmt"
	"math/rand"
	"time"
)

func main() {
	// number of desired workers
	numWorkers := 10
	StartSupervisor(numWorkers, work, shutdown)
}

func shutdown(id int) {
	// simulate handling the context being cancelled
	now := time.Now()
	fmt.Printf("Worker %d cancelled\n", id)
	time.Sleep(time.Duration(rand.Intn(5)) * time.Second)
	fmt.Printf("Worker %d shutdown with cancel, after %.1f.\n", id, time.Since(now).Seconds())
}

func work(id int) (err error) {
	// Now do whatever work we should do.
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
		fmt.Printf("Worker %d completed with %d.\n", id, q)
	}
	return nil
}

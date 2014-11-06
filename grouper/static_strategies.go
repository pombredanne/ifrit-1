package grouper

import "os"

/*
NewParallel creates a static group which starts it's members simultaneously.
Use a parallel group to describe a set of concurrent but independent processes.
*/
func NewParallel(signal os.Signal, members []Member) StaticGroup {
	return NewStatic(signal, members, parallelInit)
}

func parallelInit(members Members, client DynamicClient) {
	insert := client.Inserter()
	closed := client.CloseNotifier()

	for _, member := range members {
		select {
		case insert <- member:
		case <-closed:
			return
		}
	}

	client.Close()

	for _ = range client.EntranceListener() {
		// wait for all members to be ready
	}
}

/*
NewSerial creates a static group which starts it's members in order, each
member starting when the previous completes.  Use a serial group to describe
a pipeline of sequential processes.  Receiving s signal or a member exiting
with an error aborts the pipeline.
*/
func NewSerial(members []Member) StaticGroup {
	return NewStatic(nil, members, serialInit)
}

func serialInit(members Members, client DynamicClient) {
	exitEvents := client.ExitListener()
	insert := client.Inserter()
	closed := client.CloseNotifier()

	for _, member := range members {
		select {
		case insert <- member:
		case <-closed:
			return
		}

		exit := <-exitEvents
		if exit.Err != nil {
			return
		}
	}
}

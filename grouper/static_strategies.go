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
NewOrdered creates a static group which starts it's members in order, each
member starting when the previous becomes ready.  Use an ordered group to
describe a list of dependent processes, where each process depends upon the
previous being available in order to function correctly.
*/
func NewOrdered(terminationSignal os.Signal, members []Member) StaticGroup {
	return orderedGroup{
		terminationSignal: terminationSignal,
		pool:              NewDynamic(nil, len(members), len(members)),
		Members:           members,
	}
}

type orderedGroup struct {
	terminationSignal os.Signal
	pool              DynamicGroup
	Members
}

func (g orderedGroup) Client() StaticClient {
	return g.pool.Client()
}

func (g orderedGroup) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	err := g.Members.Validate()
	if err != nil {
		return err
	}

	go g.pool.Run(nil, make(chan<- struct{}))

	return g.orderedStrategy(signals, ready)
}

func (g orderedGroup) orderedStrategy(signals <-chan os.Signal, ready chan<- struct{}) error {
	bufferSize := len(g.Members)
	client := g.pool.Client()
	entranceEvents := client.EntranceListener()
	insert := client.Inserter()
	closed := client.CloseNotifier()
	exitTrace := make(ErrorTrace, 0, bufferSize)
	exitListener := client.ExitListener()
	var signal os.Signal
	println("starting", exitListener)
	for _, member := range g.Members {
		select {
		case insert <- member:
		case exit := <-exitListener:
			println("Saw Exit", exit.Member.Name)
			exitTrace = append(exitTrace, exit)
			return traceExitEvents(exitTrace, exitListener)

		case <-closed:
			return traceExitEvents(exitTrace, exitListener)
		}
		<-entranceEvents
	}
	println("after start")
	client.Close()
	close(ready)

	if g.terminationSignal == nil {
		return traceExitEvents(exitTrace, exitListener)
	}

	select {
	case exit := <-exitListener:
		signal = g.terminationSignal
		exitTrace = append(exitTrace, exit)
	case signal = <-signals:
	}

	for i := len(g.Members) - 1; i >= 0; i-- {
		p, ok := client.Get(g.Members[i].Name)
		if ok {
			p.Signal(signal)
			<-p.Wait()
		}
	}

	return traceExitEvents(exitTrace, exitListener)
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

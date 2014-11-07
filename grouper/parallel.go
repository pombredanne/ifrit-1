package grouper

import (
	"os"
	"reflect"

	"github.com/tedsuo/ifrit"
)

/*
NewParallel starts it's members simultaneously.  Use a parallel group to describe a set
of concurrent but independent processes.
*/
func NewParallel(terminationSignal os.Signal, members Members) ifrit.Runner {
	return parallelGroup{
		terminationSignal: terminationSignal,
		pool:              make(map[string]ifrit.Process),
		members:           members,
	}
}

type parallelGroup struct {
	terminationSignal os.Signal
	pool              map[string]ifrit.Process
	members           Members
}

func (g parallelGroup) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	err := g.validate()
	if err != nil {
		return err
	}

	signal, errTrace := g.parallelStart(signals)
	if errTrace != nil {
		return g.stop(g.terminationSignal, errTrace)
	}

	if signal != nil {
		return g.stop(signal, errTrace)
	}

	close(ready)

	signal = g.waitForSignal(signals)
	return g.stop(signal, errTrace)
}

func (o parallelGroup) validate() error {
	return o.members.Validate()
}

func (g *parallelGroup) parallelStart(signals <-chan os.Signal) (os.Signal, ErrorTrace) {
	numMembers := len(g.members)

	processes := make([]ifrit.Process, numMembers)
	cases := make([]reflect.SelectCase, 2*numMembers+1)

	for i, member := range g.members {
		process := ifrit.Background(member)

		processes[i] = process

		cases[2*i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(process.Wait()),
		}

		cases[2*i+1] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(process.Ready()),
		}
	}

	cases[2*numMembers] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(signals),
	}

	numReady := 0
	for {
		chosen, recv, _ := reflect.Select(cases)

		switch {
		case chosen == 2*numMembers:
			return recv.Interface().(os.Signal), nil
		case chosen%2 == 0:
			recvError, _ := recv.Interface().(error)
			return nil, ErrorTrace{ExitEvent{Member: g.members[chosen/2], Err: recvError}}
		default:
			cases[chosen].Chan = reflect.Zero(cases[chosen].Chan.Type())
			g.pool[g.members[chosen/2].Name] = processes[chosen/2]
			numReady++
			if numReady == numMembers {
				return nil, nil
			}
		}
	}
}

func (g *parallelGroup) waitForSignal(signals <-chan os.Signal) os.Signal {
	cases := make([]reflect.SelectCase, 0, len(g.pool)+1)
	for _, process := range g.pool {
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(process.Wait()),
		})
	}
	cases = append(cases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(signals),
	})

	chosen, recv, _ := reflect.Select(cases)
	if chosen == len(cases)-1 {
		return recv.Interface().(os.Signal)
	}

	return g.terminationSignal
}

func (g *parallelGroup) stop(signal os.Signal, errTrace ErrorTrace) ErrorTrace {
	errOccurred := len(errTrace) > 0

	cases := make([]reflect.SelectCase, 0, len(g.members))
	liveMembers := make([]Member, 0, len(g.members))
	for _, member := range g.members {
		if process, ok := g.pool[member.Name]; ok {
			process.Signal(signal)

			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(process.Wait()),
			})

			liveMembers = append(liveMembers, member)
		}
	}

	numExited := 0
	for {
		chosen, recv, _ := reflect.Select(cases)
		cases[chosen].Chan = reflect.Zero(cases[chosen].Chan.Type())
		recvError, _ := recv.Interface().(error)

		errTrace = append(errTrace, ExitEvent{
			Member: liveMembers[chosen],
			Err:    recvError,
		})

		if recvError != nil {
			errOccurred = true
		}

		numExited++
		if numExited == len(cases) {
			break
		}
	}

	if errOccurred {
		return errTrace
	}

	return nil
}

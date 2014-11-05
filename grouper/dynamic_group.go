package grouper

import (
	"fmt"
	"os"
	"sort"

	"github.com/tedsuo/ifrit"
)

/*
A DynamicGroup begins empty, and runs members as they are inserted. A
dynamic group will continue to run, even when there are no members running
within it, until it is signaled to stop. Once a dynamic group is signaled to
stop, it will no longer accept new members, and waits for the currently running
members to complete before exiting.
*/
type DynamicGroup interface {
	ifrit.Runner
	Client() DynamicClient
}

type dynamicGroup struct {
	ordered  bool
	client   dynamicClient
	signal   os.Signal
	poolSize int
}

/*
NewDynamic creates a DynamicGroup.

The maxCapacity argument sets the maximum number of concurrent processes.

The eventBufferSize argument sets the number of entrance and exit events to be
retained by the system.  When a new event listener attaches, it will receive
any previously emitted events, up to the eventBufferSize.  Older events will be
thrown away.  The event buffer is meant to be used to avoid race conditions when
the total number of members is known in advance.

The signal argument sets the termination signal.  If a member exits before
being signaled, the group propogates the termination signal.  A nil termination
signal is not propogated.
*/
func NewDynamic(signal os.Signal, maxCapacity int, eventBufferSize int, ordered bool) DynamicGroup {
	return &dynamicGroup{
		ordered:  ordered,
		client:   newClient(eventBufferSize),
		poolSize: maxCapacity,
		signal:   signal,
	}
}

func (p *dynamicGroup) Client() DynamicClient {
	return p.client
}

func (p *dynamicGroup) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	processes := newProcessSet(p.ordered)
	insertEvents := p.client.insertEventListener()
	closeNotifier := p.client.CloseNotifier()
	entranceEvents := make(entranceEventChannel)
	exitEvents := make(exitEventChannel)

	invoking := 0
	close(ready)

	for {
		select {
		case shutdown := <-signals:
			processes.Signal(shutdown)
			p.client.Close()

		case <-closeNotifier:
			closeNotifier = nil
			insertEvents = nil
			if processes.Length() == 0 {
				return p.client.closeBroadcasters()
			}
			if invoking == 0 {
				p.client.closeEntranceBroadcaster()
			}

		case newMember, ok := <-insertEvents:
			if !ok {
				p.client.Close()
				insertEvents = nil
				break
			}

			process := ifrit.Background(newMember)
			processes.Add(newMember.Name, process)

			if processes.Length() == p.poolSize {
				insertEvents = nil
			}

			invoking++

			go waitForEvents(newMember, process, entranceEvents, exitEvents)

		case entranceEvent := <-entranceEvents:
			invoking--
			p.client.broadcastEntrance(entranceEvent)

			if closeNotifier == nil && invoking == 0 {
				p.client.closeEntranceBroadcaster()
				entranceEvents = nil
			}

		case exitEvent := <-exitEvents:
			processes.Remove(exitEvent.Member.Name)
			p.client.broadcastExit(exitEvent)

			if !processes.Signaled() && p.signal != nil {
				processes.Signal(p.signal)
				p.client.Close()
				insertEvents = nil
			}

			if processes.Complete() || (processes.Length() == 0 && insertEvents == nil) {
				return p.client.closeBroadcasters()
			}

			if !processes.Signaled() {
				insertEvents = p.client.insertEventListener()
			}
		}
	}
}

func waitForEvents(
	member Member,
	process ifrit.Process,
	entrance entranceEventChannel,
	exit exitEventChannel,
) {
	select {
	case <-process.Ready():
		entrance <- EntranceEvent{
			Member:  member,
			Process: process,
		}

		exit <- ExitEvent{
			Member: member,
			Err:    <-process.Wait(),
		}

	case err := <-process.Wait():
		entrance <- EntranceEvent{
			Member:  member,
			Process: process,
		}

		exit <- ExitEvent{
			Member: member,
			Err:    err,
		}
	}
}

type processElement struct {
	index   int
	process ifrit.Process
}

type processElementSlice []processElement

func (p processElementSlice) Len() int           { return len(p) }
func (p processElementSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p processElementSlice) Less(i, j int) bool { return p[i].index > p[j].index }

type processSet struct {
	ordered   bool
	count     int
	processes map[string]processElement
	shutdown  os.Signal
}

func newProcessSet(ordered bool) *processSet {
	return &processSet{
		ordered:   ordered,
		processes: map[string]processElement{},
	}
}

func (g *processSet) Signaled() bool {
	return g.shutdown != nil
}

func (g *processSet) Signal(signal os.Signal) {
	g.shutdown = signal

	var pSlice processElementSlice
	for _, p := range g.processes {
		pSlice = append(pSlice, p)
	}

	if g.ordered {
		sort.Sort(pSlice)
	}

	for _, p := range pSlice {
		p.process.Signal(signal)
		if g.ordered {
			p.process.Wait()
		}
	}
}

func (g *processSet) Length() int {
	return len(g.processes)
}

func (g *processSet) Complete() bool {
	return len(g.processes) == 0 && g.shutdown != nil
}

func (g *processSet) Add(name string, process ifrit.Process) {
	_, ok := g.processes[name]
	if ok {
		panic(fmt.Errorf("member inserted twice: %#v", name))
	}
	g.processes[name] = processElement{
		process: process,
		index:   g.count,
	}

	g.count++
}

func (g *processSet) Remove(name string) {
	delete(g.processes, name)
}

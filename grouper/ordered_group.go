package grouper

import (
	"os"

	"github.com/tedsuo/ifrit"
)

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
		shutdown:          make(chan struct{}),
	}
}

type orderedGroup struct {
	terminationSignal os.Signal
	pool              DynamicGroup
	shutdown          chan struct{}
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

	ifrit.Background(g.pool)

	go func() {
		g.orderedStart()
		close(ready)
	}()

	go g.waitForSignal(signals)

	client := g.pool.Client()
	return traceExitEvents(make(ErrorTrace, 0, len(g.Members)), client.ExitListener())
}

func (g orderedGroup) orderedStart() {
	client := g.pool.Client()
	entranceEvents := client.EntranceListener()
	insert := client.Inserter()
	closed := client.CloseNotifier()

	for _, member := range g.Members {
		select {
		case insert <- member:
		case <-closed:
			return
		}

		<-entranceEvents
	}

	client.Close()
}

func (g orderedGroup) waitForSignal(signals <-chan os.Signal) {
	client := g.pool.Client()
	memberExit := client.ExitListener()

	for {
		select {
		case signal := <-signals:
			g.orderedStop(signal)
			return

		case _, ok := <-memberExit:
			if !ok {
				return
			}
			if g.terminationSignal != nil {
				client.Close()
				g.orderedStop(g.terminationSignal)
				return
			}
		}
	}
}

func (g orderedGroup) orderedStop(signal os.Signal) {
	client := g.pool.Client()
	client.Close()

	for i := len(g.Members) - 1; i >= 0; i-- {
		p, ok := client.Get(g.Members[i].Name)
		if ok {
			p.Signal(signal)
			<-p.Wait()
		}
	}
}

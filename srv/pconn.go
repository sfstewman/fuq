package srv

import (
	"context"
	"fmt"
	"github.com/sfstewman/fuq"
	"github.com/sfstewman/fuq/proto"
	"github.com/sfstewman/fuq/websocket"
	"log"
	"sync"
)

// TODO: need tests to check failure cases
type persistentConn struct {
	mu sync.Mutex

	M *websocket.Messenger
	C *proto.Conn
	F *Foreman

	NodeInfo fuq.NodeInfo

	ready       chan struct{}
	helloSignal chan struct{}
	stopSignal  chan struct{}

	// canceled channel
	Canceled chan []fuq.TaskPair

	nproc, nrun, nstop uint16
}

func newPersistentConn(f *Foreman, ni fuq.NodeInfo, messenger *websocket.Messenger) *persistentConn {
	pconn := proto.NewConn(proto.Opts{
		Messenger: messenger,
		Flusher:   proto.NopFlusher{},
		Worker:    false,
	})

	pc := &persistentConn{
		M:           messenger,
		C:           pconn,
		F:           f,
		NodeInfo:    ni,
		helloSignal: make(chan struct{}),
		stopSignal:  make(chan struct{}),
		Canceled:    make(chan []fuq.TaskPair),
	}

	pconn.OnMessageFunc(proto.MTypeHello, pc.onHello)
	pconn.OnMessageFunc(proto.MTypeUpdate, pc.onUpdate)

	return pc
}

func (pc *persistentConn) onHello(msg proto.Message) proto.Message {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	hello := msg.Data.(proto.HelloData)

	// XXX - should record running tasks

	// XXX - check for overflow!
	pc.nproc = uint16(hello.NumProcs)
	pc.nrun = uint16(len(hello.Running))
	pc.ready = nil

	log.Printf("srv.PConn(%p): received HELLO(%d|%d)",
		pc, pc.nproc, pc.nrun)
	log.Printf("srv.PConn(%p): running tasks = %v",
		pc, hello.Running)

	repl := proto.OkayMessage(pc.nproc, pc.nrun, msg.Seq)
	helloSignal := pc.helloSignal
	repl.After(func() {
		log.Printf("Sent reply to HELLO: %#v", repl)
		close(helloSignal)
	})

	return repl
}

// for uint16 overflow detection
const MaxUint16 uint16 = ^uint16(0)

func (pc *persistentConn) onUpdate(msg proto.Message) proto.Message {
	log.Printf("received UPDATE %v", msg)

	pc.mu.Lock()
	defer pc.mu.Unlock()

	upd := msg.Data.(fuq.JobStatusUpdate)

	if err := pc.F.UpdateTaskStatus(upd); err != nil {
		log.Printf("error updating task status (update=%v): %v",
			upd, err)
	}

	// check for overflow/underflow
	switch {
	case pc.nproc == MaxUint16:
		panic(fmt.Sprintf("nproc will overflow (currently %d)", pc.nproc))
	case pc.nrun == 0:
		panic(fmt.Sprintf("nrun will underflow (currently %d)", pc.nrun))
	}

	// TODO - jobs that occupy more than one core
	if pc.nstop > 0 {
		pc.nstop--
	} else {
		pc.nproc++
	}
	pc.nrun--

	log.Printf("onUpdate: nproc=%d, nrun=%d, nstop=%d", pc.nproc, pc.nrun, pc.nstop)
	log.Printf("pconn(%p): UPDATE received", pc)
	if pc.ready != nil {
		log.Printf("pconn(%p): signaling READY", pc)
		close(pc.ready)
		pc.ready = nil
	}

	reply := proto.OkayMessage(pc.nproc, pc.nrun, msg.Seq)

	if pc.nproc == 0 && pc.nrun == 0 {
		reply.After(func() {
			close(pc.stopSignal)
		})
	}

	log.Printf("pconn(%p): sending WAKEUP", pc)
	pc.F.WakeupListeners()
	return reply
}

func (pc *persistentConn) numProcAvail() (int, chan struct{}) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	nproc := int(pc.nproc)
	if nproc > 0 || pc.nrun == 0 {
		return nproc, nil
	}

	if pc.ready == nil {
		pc.ready = make(chan struct{})
	}
	ready := pc.ready

	return 0, ready
}

/*
waitOnWorkers:
	select {
	case <-ready:
		continue

	case <-wakeup:
		if f.IsNodeShutdown(ni.UniqName) {
			return 0, nil
		}

	case err := <-errCh:
		log.Printf("pconn(%p): received error %v from errCh", pc, err)
		if err != nil {
			return 0, err
		}
		return 0, proto.ErrClosed

	case <-ctx.Done():
		return 0, ctx.Err()
	}

waitForStop:
	select {
	case <-stopSignal:
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}

Loop():
	if len(tasks) == 0 {
		tasksAvail := f.ready(false)
		f.jobsSignal.mu.Unlock()

		select {
		case <-tasksAvail:
			log.Printf("pc(%p): tasksAvail signal", pc)
			continue

		case <-loopCtx.Done():
			break loop
		case err := <-errCh:
			return err
		}
	}
*/

func (pc *persistentConn) sendStop(ctx context.Context) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	nstop := uint32(pc.nproc) + uint32(pc.nrun)

	resp, err := pc.C.SendStop(ctx, nstop)
	if err != nil {
		return err
	}

	if resp.Type != proto.MTypeOK {
		return fmt.Errorf("error in reply: %v", resp)
	}

	np, nr := resp.AsOkay()

	// XXX - check for overflow
	nleft := uint32(np) + uint32(nr)
	pc.nstop = uint16(nleft)

	log.Printf("sentStop: orig(np=%d,nr=%d,ns=%d).  curr(np=%d,nr=%d,nleft=%d,ns=%d)",
		pc.nproc, pc.nrun, nstop, np, nr, nleft, pc.nstop)

	pc.nproc, pc.nrun = np, nr
	log.Printf("sentStop: nproc=%d, nrun=%d, nstop=%d", pc.nproc, pc.nrun, pc.nstop)
	return nil
}

func (pc *persistentConn) sendJobs(ctx context.Context, tasks []fuq.Task) error {
	// pc.mu.Lock()
	// defer pc.mu.Unlock()

	log.Printf("SENDING %d jobs", len(tasks))

	pc.mu.Lock()
	pc.nproc -= uint16(len(tasks))
	pc.nrun += uint16(len(tasks))
	pc.mu.Unlock()

	resp, err := pc.C.SendJob(ctx, tasks)
	if err != nil {
		return err
	}

	if resp.Type != proto.MTypeOK {
		return fmt.Errorf("error in reply: %v", resp)
	}

	np, nr := resp.AsOkay()
	_, _ = np, nr

	pc.mu.Lock()
	defer pc.mu.Unlock()

	log.Printf("sentJobs: nproc=%d, nrun=%d, reply OK(%d|%d)",
		pc.nproc, pc.nrun, np, nr)
	return nil
}

func (pc *persistentConn) sendCancel(ctx context.Context, pairs []fuq.TaskPair) error {
	log.Printf("pconn(%p): sending CANCEL(%v)", pc, pairs)

	resp, err := pc.C.SendCancel(ctx, pairs)
	if err != nil {
		return err
	}

	if resp.Type != proto.MTypeOK {
		return fmt.Errorf("error in reply: %v", resp)
	}

	nc, _ := resp.AsOkay()
	log.Printf("pconn(%p): found %d tasks to cancel", nc)

	return nil
}

func (pc *persistentConn) fetchTasks(nproc int) (tasks []fuq.Task, tasksAvail <-chan struct{}, err error) {
	f := pc.F

	f.jobsSignal.mu.Lock()
	defer f.jobsSignal.mu.Unlock()

	// request jobs
	tasks, err = f.FetchPendingTasks(nproc)
	if err != nil {
		log.Printf("error fetching tasks: %v", err)
		return
	}

	if len(tasks) > 0 {
		return
	}

	tasksAvail = f.ready(false)
	return
}

func (pc *persistentConn) Loop(ctx context.Context) error {
	errCh := make(chan error, 1)

	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		defer close(errCh)
		defer cancel()

		err := pc.C.ConversationLoop(ctx)
		if err != nil {
			errCh <- err
		}
	}()

	startSignal := pc.helloSignal
	select {
	case <-startSignal:
		/* nop */
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}

	f := pc.F
	uniqName := pc.NodeInfo.UniqName

loop:
	for {
		// events:
		//
		//   context canceled
		//   error occured
		//
		//   nodes available
		//   wakeup requested (jobs available or stop condition)
		//   job canceled
		//
		//   job sent
		//
		// three main waiting states and a sending state:
		//	1. for nodes
		//		events: nodes available, job canceled
		//
		//	2. for jobs
		//		events: job available, job canceled
		//
		//	3. to stop
		//		events: job canceled
		//
		//

		var (
			workersAvail <-chan struct{}
			tasksAvail   <-chan struct{}
			stopSignal   <-chan struct{}

			nproc int
			tasks []fuq.Task
			err   error
		)

		nproc, workersAvail = pc.numProcAvail()

		// check if node is shut down
		if f.IsNodeShutdown(uniqName) {
			if err := pc.sendStop(loopCtx); err != nil {
				log.Printf("error sending STOP: %v", err)
				return err
			}

			if nproc == 0 && workersAvail == nil {
				// we're not waiting for workers, so
				// just exit
				return nil
			}

			stopSignal = pc.stopSignal

			// waiting for the stop signal; don't bother
			// waiting for workers or tasks
			goto wait_on_event
		}

		if nproc == 0 {
			// no workers, so skip fetching tasks
			goto wait_on_event
		}

		tasks, tasksAvail, err = pc.fetchTasks(nproc)
		if err != nil {
			log.Printf("error fetching tasks: %v", err)
			return err
		}

		if len(tasks) > 0 {
			if err := pc.sendJobs(loopCtx, tasks); err != nil {
				log.Printf("error sending tasks: %v", err)
				return err
			}

			// XXX - update running list
			log.Printf("%s: queued %d tasks",
				pc.NodeInfo.UniqName, len(tasks))

			continue
		}

	wait_on_event:
		select {
		case <-loopCtx.Done():
			log.Printf("pconn(%p): context canceled (%v)", pc, loopCtx.Err())
			break loop

		case err := <-errCh:
			log.Printf("pconn(%p): error (%v)", pc, err)
			return err

		case <-stopSignal:
			log.Printf("pconn(%p): STOP signaled", pc)
			break loop

		case pairs := <-pc.Canceled:
			if err := pc.sendCancel(loopCtx, pairs); err != nil {
				log.Printf("pconn(%p): error sending CANCEL: %v", pc, err)
				return err
			}
			continue loop

		case <-workersAvail:
			log.Printf("pc(%p): workers available", pc)
			continue loop

		case <-tasksAvail:
			log.Printf("pc(%p): jobs available", pc)
			continue loop
		}

	}

	// check that we didn't miss an error
	select {
	case err := <-errCh:
		return err
	default:
		if err := loopCtx.Err(); err != nil {
			return err
		}
	}
	return nil
}

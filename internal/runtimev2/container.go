// +build linux

package runtimev2

import (
	"context"
	"errors"
	"sync"
	"syscall"

	"github.com/Microsoft/opengcs/service/gcs/gcserr"
	"github.com/containerd/go-runc"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

// Container is an object that represents the HCS container concept. In Linux
// this concept doesn't map directly since the container is a process. To match
// Linux we treat a container as the lifetime of the the init process given to
// the container and tie the lifetimes together.
//
// IE: Unlike on Windows you can not have a container that does not have a
// process.
type Container struct {
	// id is the runc id of the container
	//
	// This MUST be treated as readonly in the lifetime of the object.
	id   string
	init *Process
	r    *runc.Runc

	pl        sync.Mutex
	processes map[int]*Process
}

// TODO: Create?

// Start starts an already created container.
//
// TODO: What is the correct error if already started to return to HCS.
func (c *Container) Start(ctx context.Context) (err error) {
	activity := "runtimev2::Container::Start"
	log := logrus.WithFields(logrus.Fields{
		"cid": c.id,
	})
	log.Debug(activity + " - Begin Operation")
	defer func() {
		if err != nil {
			log.Data[logrus.ErrorKey] = err
			log.Error(activity + " - End Operation")
		} else {
			log.Debug(activity + " - End Operation")
		}
	}()
	// TODO: Handle the state machine
	return c.r.Start(ctx, c.id)
}

/*
// Shutdown is a Windows container silo concept that most closely simulates
// sending a `CtrlShutdown` to all processes in the process tree. To simulate
// this for Linux we send `unix.SIGTERM`.
func (c *Container) Shutdown(ctx context.Context) (err error) {
	activity := "runtimev2::Container::Shutdown"
	log := logrus.WithFields(logrus.Fields{
		"cid": c.id,
	})
	log.Debug(activity + " - Begin Operation")
	defer func() {
		if err != nil {
			log.Data[logrus.ErrorKey] = err
			log.Error(activity + " - End Operation")
		} else {
			log.Debug(activity + " - End Operation")
		}
	}()

	return c.r.Kill(ctx, c.id, unix.SIGTERM, &runc.KillOpts{all: true})
}

// Terminate is a Windows container silo concept that most closely simulates
// physically terminating the process. On Linux we simulate this by cascading
// `unix.SIGKILL`.
func (c *Container) Terminate(ctx context.Context) (err error) {
	activity := "runtimev2::Container::Terminate"
	log := logrus.WithFields(logrus.Fields{
		"cid": c.id,
	})
	log.Debug(activity + " - Begin Operation")
	defer func() {
		if err != nil {
			log.Data[logrus.ErrorKey] = err
			log.Error(activity + " - End Operation")
		} else {
			log.Debug(activity + " - End Operation")
		}
	}()

	return c.r.Kill(ctx, c.id, unix.SIGKILL, &runc.KillOpts{all: true})
}
*/ 

func (c *Container) SignalProcess(ctx context.Context, pid, sig int, all bool) (err error) {
	activity := "runtimev2::Container::SignalProcess"
	log := logrus.WithFields(logrus.Fields{
		"cid":    c.id,
		"pid":    pid,
		"signal": sig,
		"all":    all,
	})
	log.Debug(activity + " - Begin Operation")
	defer func() {
		if err != nil {
			log.Data[logrus.ErrorKey] = err
			log.Error(activity + " - End Operation")
		} else {
			log.Debug(activity + " - End Operation")
		}
	}()
	if all && pid != c.init.pid {
		return gcserr.WrapHresult(errors.New("cannot use 'all' when 'pid' is not the init pid"), gcserr.HrInvalidArg)
	}
	if pid == c.init.pid {
		opts := runc.KillOpts{
			all: all
		}
		return c.r.Kill(ctx, c.id, sig, opts) 
	}
	// Signals to an exec process can just be delivered to the pid itself.
	return syscall.Kill(pid, syscall.Signal(sig))
}

func (c *Container) ExecProcess(ctx context.Context, spec specs.Process) (err error) {
	activity := "runtimev2::Container::ExecProcess"
	log := logrus.WithFields(logrus.Fields{
		"cid": c.id,
	})
	log.Debug(activity + " - Begin Operation")
	defer func() {
		if err != nil {
			log.Data[logrus.ErrorKey] = err
			log.Error(activity + " - End Operation")
		} else {
			log.Debug(activity + " - End Operation")
		}
	}()
	return c.r.Exec(ctx, c.id, spec, &runc.ExecOpts{})
}

func (c *Container) GetProcess(pid int) (*Process, error) {
	activity := "runtimev2::Container::GetProcess"
	log := logrus.WithFields(logrus.Fields{
		"cid": c.id,
		"pid": pid,
	})
	log.Debug(activity + " - Begin Operation")
	defer func() {
		if err != nil {
			log.Data[logrus.ErrorKey] = err
			log.Error(activity + " - End Operation")
		} else {
			log.Debug(activity + " - End Operation")
		}
	}()

	if c.init.pid == pid {
		return c.init, nil
	}
	c.pl.Lock()
	defer c.pl.Unlock()
	p, ok := c.processes[pid]
	if !ok {
		return nil, gcserr.NewProcessDoesNotExistError(pid)
	}
	return p, nil
}

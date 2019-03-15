// +build linux

package pmem

import (
	"fmt"
	"sync"
	"syscall"

	"github.com/Microsoft/opengcs/internal/storage"
	"github.com/sirupsen/logrus"
)

var (
	// pmemDevices is a map from device path to device object.
	pmemDevices sync.Map
)

func pmemDeviceKey(deviceNumber uint) string {
	return fmt.Sprintf("%d", deviceNumber)
}

func OpenDevice(deviceNumber uint) (_ *Pmem, err error) {
	op := "pmem::OpenDevice"
	log := logrus.WithFields(
		logrus.Fields{
			"deviceNumber": deviceNumber,
		})
	log.Debug(op + " - Begin Operation")
	defer func() {
		if err != nil {
			log.Data[logrus.ErrorKey] = err
			log.Error(op + " - End Operation - Error")
		} else {
			log.Debug(op + " - End Operation - Success")
		}
	}()

	key := pmemDeviceKey(deviceNumber)
	actualI, _ := pmemDevices.LoadOrStore(key, &Pmem{
		deviceNumber: deviceNumber,
	})
	return actualI.(*Pmem), nil
}

type Pmem struct {
	deviceNumber uint

	mu       sync.Mutex
	refCount int
}

func (p *Pmem) unmount() {
	op := "pmem::Pmem::unmount"
	log := logrus.WithFields(logrus.Fields{
		"deviceNumber": p.deviceNumber,
	})
	log.Debug(op + " - Begin Operation")

	// We dont return err but capture the closure so we can log the result.
	var err error
	defer func() {
		if err != nil {
			log.Data[logrus.ErrorKey] = err
			log.Error(op + " - End Operation - Error")
		} else {
			log.Debug(op + " - End Operation - Success")
		}
	}()

	p.mu.Lock()
	defer p.mu.Unlock()

	p.refCount--
	if p.refCount < 0 {
		panic("pmem mount refcount mismatch")
	}
}

func (p *Pmem) MountTo(target string) (_ *storage.Mount, err error) {
	op := "pmem::Pmem::MountTo"
	log := logrus.WithFields(logrus.Fields{
		"deviceNumber": p.deviceNumber,
		"target":       target,
	})
	log.Debug(op + " - Begin Operation")
	defer func() {
		if err != nil {
			log.Data[logrus.ErrorKey] = err
			log.Error(op + " - End Operation - Error")
		} else {
			log.Debug(op + " - End Operation - Success")
		}
	}()

	p.mu.Lock()
	// We do not decrement this here. On error `storage.GetOrCreateMount` MUST
	// call the cleanup `p.unmount` func.
	p.refCount++
	p.mu.Unlock()

	var flags uintptr = syscall.MS_RDONLY
	data := "noload,dax"
	return storage.GetOrCreateMount(fmt.Sprintf("/dev/pmem%d", p.deviceNumber), target, "ext4", flags, data, p.unmount)
}

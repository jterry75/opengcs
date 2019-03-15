// +build linux

package scsi

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/Microsoft/opengcs/internal/storage"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	// scsiDevices is a map from device path to device object.
	scsiDevices sync.Map

	// scsiDeviceLookupTimeout is the amount of time the `Scsi` object will
	// attempt to resolve a `controller, lun` pair to a `/dev/sd*` path before
	// giving up.
	//
	// NOTE: this is not `const` so that tests can minimize the time to hit the
	// timeout code paths.
	scsiDeviceLookupTimeout = time.Second * 2
)

// Testing dependencies
var (
	ioutilReadDir = ioutil.ReadDir
	osOpenFile    = openFileIoWriteCloser
)

func openFileIoWriteCloser(path string, flag int, perm os.FileMode) (io.WriteCloser, error) {
	return os.OpenFile(path, flag, perm)
}

func scsiDevicesKey(controller, lun uint8) string {
	return fmt.Sprintf("0:0:%d:%d", controller, lun)
}

// OpenDevice gets or creates a `Scsi` object that is fully resolved to
// `controller, lun`.
//
// This call is safe for two threads calling with the same `controller, lun`
// pairs and will return the same `Scsi` object or error.
func OpenDevice(controller, lun uint8) (_ *Scsi, err error) {
	op := "scsi::OpenDevice"
	log := logrus.WithFields(
		logrus.Fields{
			"controller": controller,
			"lun":        lun,
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

	key := scsiDevicesKey(controller, lun)
	actualI, _ := scsiDevices.LoadOrStore(key, &Scsi{
		key:        key,
		controller: controller,
		lun:        lun,
	})
	actual := actualI.(*Scsi)
	if err := actual.resolve(); err != nil {
		scsiDevices.Delete(key)
		return nil, err
	}
	return actual, nil
}

// Scsi is a type that allows for mapping SCSI block device to active mount
// locations.
//
// A SCSI device with zero mounts does not need to be ejected as there is no
// data that might need to be synchronized. Therefore a device is ejected only
// when the last mount is unmounted by protocol.
type Scsi struct {
	key             string
	controller, lun uint8

	mu           sync.Mutex
	source       string
	resolveError error
	refCount     int
}

// resolve will find `s.controller, s.lun` and resolve it to its `/dev/sd*`
// path.
//
// Note: This is safe to call multiple times and the operation will happen
// exactly once. If the operation fails all subsequent calls will fail with the
// same error.
func (s *Scsi) resolve() (err error) {
	op := "scsi::Scsi::resolve"
	log := logrus.WithFields(logrus.Fields{
		"controller": s.controller,
		"lun":        s.lun,
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

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.source != "" {
		// Another thread already resolved
		return nil
	} else if s.resolveError != nil {
		return s.resolveError
	}

	var deviceNames []os.FileInfo
	now := time.Now()
	path := filepath.Join("/sys/bus/scsi/devices", s.key, "block")
	for {
		deviceNames, err = ioutilReadDir(path)
		if err != nil {
			if !os.IsNotExist(err) {
				s.resolveError = err
				return err
			}
			// Its a not exist. Wait for success if we have time.
			if time.Since(now) > scsiDeviceLookupTimeout {
				s.resolveError = errors.Wrapf(err, "timed out waiting for SCSI device: %s", path)
				return s.resolveError
			}
			time.Sleep(time.Millisecond * 10)
		} else {
			break
		}
	}
	switch len(deviceNames) {
	case 0:
		s.resolveError = fmt.Errorf("no matching device names found for SCSI device: %s", path)
		return s.resolveError
	case 1:
		s.source = filepath.Join("/dev", deviceNames[0].Name())
		return nil
	default:
		s.resolveError = fmt.Errorf("more than one block device could match SCSI device: %s", path)
		return s.resolveError
	}
}

// eject ejects `s` when the last `Mount` is removed. The caller does not
// directly call `eject` but rather `Mount.Unmount`.
func (s *Scsi) eject() {
	op := "scsi::Scsi::eject"
	log := logrus.WithFields(logrus.Fields{
		"controller": s.controller,
		"lun":        s.lun,
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

	s.mu.Lock()
	defer s.mu.Unlock()

	s.refCount--
	if s.refCount < 0 {
		panic("scsi mount refcount mismatch")
	} else if s.refCount == 0 {
		// There are no more mounts. We eject the disk
		path := filepath.Join("/sys/bus/scsi/devices", s.key, "delete")
		var f io.WriteCloser
		f, err = osOpenFile(path, os.O_WRONLY, 0644)
		if err != nil {
			err = errors.Wrap(err, "failed to open scsi device for eject")
		} else {
			defer f.Close()
			_, err = f.Write([]byte("1\n"))
			if err != nil {
				err = errors.Wrap(err, "failed to write to scsi device for eject")
			}
		}

		// Cleanup our in-memory cache
		scsiDevices.Delete(s.source)
	}
}

// MountTo mounts `s` to `target`. If `s` is already mounted to `target` returns
// the same mount with an incremented ref count. When the caller is done with
// its use of the mount they must call `Mount.Unmount`.
func (s *Scsi) MountTo(target string, readonly bool) (_ *storage.Mount, err error) {
	op := "scsi::Scsi::MountTo"
	log := logrus.WithFields(logrus.Fields{
		"controller": s.controller,
		"lun":        s.lun,
		"target":     target,
		"readonly":   readonly,
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

	s.mu.Lock()
	// We do not decrement this here. On error `storage.GetOrCreateMount` MUST
	// call the cleanup `s.eject` func.
	s.refCount++
	s.mu.Unlock()

	var flags uintptr
	data := ""
	if readonly {
		flags |= syscall.MS_RDONLY
		data = "noload"
	}

	return storage.GetOrCreateMount(s.source, target, "ext4", flags, data, s.eject)
}

/*
func GetOrCreateScsiMount(controller, lun uint8, target string, readonly bool) (*Mount, error) {
	mount := &Mount{
		target: target,
	}
	// Hold the lock while the `LoadOrStore` takes place to
	mount.mu.Lock()

	actual, loaded := scsiMounts.LoadOrStore(target, mount)
	// This mount lost the race. Release it
	if loaded {
		mount.mu.Unlock()
	}
	// Always use actual as it is the valid in-memory mount. If another thread
	// has already mounted this is a no-op or else it will return the same
	// error.
	err := actual.mount()
	if err != nil {
		// Clear this invalid mount
		scsiMounts.Delete(target)
		return nil, err
	}
	return actual, nil
}
*/

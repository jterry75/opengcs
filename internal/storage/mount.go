// +build linux

package storage

import (
	"sync"
	"syscall"

	"github.com/pkg/errors"
)

var (
	// ErrPathNotMounted is a typed error returned when a Mount is Unmounted more than once.
	ErrPathNotMounted = errors.New("path not mounted")
)

// Testing dependencies
var (
	syscallMount   = syscall.Mount
	syscallUnmount = syscall.Unmount
)

func GetOrCreateMount(source, target, fstype string, flags uintptr, data string, cleanup func()) (*Mount, error) {
	if err := syscallMount(source, target, fstype, flags, data); err != nil {
		return nil, errors.Wrapf(err, "failed to mount source: '%s' to target: '%s'", source, target)
	}
	return &Mount{
		mounted: true,
		target:  target,
	}, nil
}

// Mount is a structure that represents a user created mount.
type Mount struct {
	target string

	mu       sync.Mutex
	mounted  bool
	refCount int
}

// Target is the file system location `m` is mounted to.
func (m *Mount) Target() string {
	return m.target
}

// NewMount creates a pointer to a mount location
func NewMount(source, target, fstype string, flags uintptr, data string) (*Mount, error) {
	if err := syscallMount(source, target, fstype, flags, data); err != nil {
		return nil, errors.Wrapf(err, "failed to mount source: '%s' to target: '%s'", source, target)
	}
	return &Mount{
		mounted: true,
		target:  target,
	}, nil
}

// IsMounted returns true if the target is mounted
func (m *Mount) IsMounted() bool {
	return m.mounted
}

// Unmount unmounts the target. If the target is not mounted
// returns ErrPathNotMounted
func (m *Mount) Unmount(flags int) error {
	if !m.mounted {
		return ErrPathNotMounted
	}

	if err := syscallUnmount(m.target, flags); err != nil {
		return errors.Wrapf(err, "failed to unmount path: %s", m.target)
	}

	m.mounted = false
	return nil
}

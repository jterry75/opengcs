// +build linux

package storage

import (
	"testing"

	"github.com/pkg/errors"
)

func Test_NewMount_Success(t *testing.T) {
	// Clear to verify not called
	syscallUnmount = nil

	s := "fakesource"
	tar := "faketarget"
	fst := "fakefstype"
	f := uintptr(2)
	d := "fakedata"
	syscallMount = func(source, target, fstype string, flags uintptr, data string) error {
		if s != source {
			t.Fatal("new mount didnt pass valid source")
		}
		if tar != target {
			t.Fatal("new mount didnt pass valid target")
		}
		if fst != fstype {
			t.Fatal("new mount didnt pass valid fstype")
		}
		if f != flags {
			t.Fatal("new mount didnt pass valid flags")
		}
		if d != data {
			t.Fatal("new mount didnt pass valid data")
		}
		return nil
	}

	var mnt *Mount
	var err error
	if mnt, err = NewMount(s, tar, fst, f, d); err != nil {
		t.Fatal(err)
	}
	if !mnt.mounted {
		t.Fatal("mnt does not have mounted flag true")
	}
	if mnt.target != tar {
		t.Fatal("unmount target is not correct value")
	}
}

func Test_NewMount_Error(t *testing.T) {
	// Clear to verify not called
	syscallUnmount = nil

	syscallMount = func(source, target, fstype string, flags uintptr, data string) error {
		return errors.New("fake mount error")
	}

	mnt, err := NewMount("badsource", "badtarget", "badfstype", uintptr(0), "baddata")
	if err == nil {
		t.Fatal("mount should have failed")
	}
	if mnt != nil {
		t.Fatal("mount should not return valid mount pointer")
	}
}

func Test_IsMounted_Mounted(t *testing.T) {
	// Clear to verify not called
	syscallUnmount = nil

	syscallMount = func(source, target, fstype string, flags uintptr, data string) error {
		return nil
	}

	mnt, err := NewMount("fakesource", "target", "fakefstype", uintptr(0), "fakedata")
	if err != nil {
		t.Fatal("mount should have succeeded")
	}
	if !mnt.IsMounted() {
		t.Fatal("mount should show as mounted if created successfully")
	}
}

func Test_IsMounted_Unmount_NotMounted(t *testing.T) {
	syscallMount = func(source, target, fstype string, flags uintptr, data string) error {
		return nil
	}
	syscallUnmount = func(target string, flags int) error {
		// simulate unmount success
		return nil
	}

	mnt, err := NewMount("fakesource", "target", "fakefstype", uintptr(0), "fakedata")
	if err != nil {
		t.Fatal("mount should have succeeded")
	}
	err = mnt.Unmount(0)
	if err != nil {
		t.Fatal("unmount should have succeeded")
	}
	if mnt.IsMounted() {
		t.Fatal("mount should show as unmounted after call to unmount successfully")
	}
}

func Test_Unmount_NotMounted_Error(t *testing.T) {
	// Clear to verify not called
	syscallMount = nil

	mnt := &Mount{
		mounted: false,
		target:  "faketarget",
	}

	err := mnt.Unmount(0)
	if err != ErrPathNotMounted {
		t.Fatal("unmounted mount should return ErrPathNotMounted")
	}
}

func Test_Unmount_Mounted_Success(t *testing.T) {
	// Clear to verify not called
	syscallMount = nil

	tar := "faketarget"
	f := 12
	syscallUnmount = func(target string, flags int) error {
		// simulate unmount success
		if target != tar {
			t.Fatal("invalid target to unmount")
		}
		if flags != f {
			t.Fatal("invalid flags to unmount")
		}
		return nil
	}

	mnt := &Mount{
		mounted: true,
		target:  tar,
	}

	err := mnt.Unmount(f)
	if err != nil {
		t.Fatal("unmount should return nil error on success")
	}
	if mnt.mounted {
		t.Fatal("unmount should set mounted state to false on success")
	}
}

func Test_Unmount_Mounted_Error(t *testing.T) {
	// Clear to verify not called
	syscallMount = nil

	syscallUnmount = func(target string, flags int) error {
		// simulate unmount error
		return errors.New("fake unmount error")
	}

	mnt := &Mount{
		mounted: true,
		target:  "faketarget",
	}

	err := mnt.Unmount(0)
	if err == nil {
		t.Fatal("unmount should return error on failure")
	}
	if !mnt.mounted {
		t.Fatal("unmount should not modify mounted state on failure")
	}
}

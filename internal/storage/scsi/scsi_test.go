// +build linux

package scsi

import (
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
)

var _ = (os.FileInfo)(&fakeFileInfo{})

// fakeFileInfo is used to fake out a call to ioutil.ReadDir.
type fakeFileInfo struct {
	name string
}

func (ffi *fakeFileInfo) Name() string {
	return ffi.name
}

func (ffi *fakeFileInfo) Size() int64 {
	return 0
}

func (ffi *fakeFileInfo) Mode() os.FileMode {
	return os.ModeDir
}

func (ffi *fakeFileInfo) ModTime() time.Time {
	return time.Now()
}

func (ffi *fakeFileInfo) IsDir() bool {
	return true
}

func (ffi *fakeFileInfo) Sys() interface{} {
	return nil
}

// forceCleanup removes any cached value from the `scsiDevices` map for a clean
// state per test.
func forceCleanup(controller, lun uint8) {
	scsiDevices.Delete(scsiDevicesKey(controller, lun))
}

func Test_OpenDevice_Failure(t *testing.T) {
	ioutilReadDir = func(dirname string) ([]os.FileInfo, error) {
		return nil, errors.New("intentional failure")
	}

	var controller uint8 = 1
	var lun uint8 = 1

	defer forceCleanup(controller, lun)

	s, err := OpenDevice(controller, lun)
	if err == nil {
		t.Fatal("expected failure got nil")
	}
	if s != nil {
		t.Fatalf("expected nil scsi device got: %+v", s)
	}

	// Verify there is no entry in the map
	_, loaded := scsiDevices.Load(scsiDevicesKey(controller, lun))
	if loaded {
		t.Fatal("expected map to not load based on key")
	}
}

func Test_OpenDevice_Success(t *testing.T) {
	ioutilReadDir = func(dirname string) ([]os.FileInfo, error) {
		return []os.FileInfo{
			&fakeFileInfo{
				name: "sda",
			},
		}, nil
	}

	var controller uint8 = 1
	var lun uint8 = 1

	defer forceCleanup(controller, lun)

	s, err := OpenDevice(controller, lun)
	if err != nil {
		t.Fatalf("expected nil error got %v", err)
	}
	if s == nil {
		t.Fatal("expected scsi device got nil")
	}

	// Verify there is an entry in the map
	_, loaded := scsiDevices.Load(scsiDevicesKey(controller, lun))
	if !loaded {
		t.Fatal("expected map to load based on key")
	}
}

// Test_OpenDevice_Second_Call_Same_Failure calls OpenDevice at the same time
// twice and verifies that when we simulate a failure in the first call we get
// the failure in the second call with no additional call to resolve.
func Test_OpenDevice_Second_Call_Same_Failure(t *testing.T) {
	var count int32
	wg := sync.WaitGroup{}
	wg.Add(1)
	ioutilReadDir = func(dirname string) ([]os.FileInfo, error) {
		new := atomic.AddInt32(&count, 1)
		if new > 1 {
			t.Errorf("ioutil.ReadDir called %d times", new)
		}
		wg.Wait()
		return nil, errors.New("intentional failure")
	}

	var controller uint8 = 1
	var lun uint8 = 1

	defer forceCleanup(controller, lun)

	type tuple struct {
		s *Scsi
		e error
	}
	one := make(chan tuple, 1)
	go func() {
		s, err := OpenDevice(controller, lun)
		one <- tuple{s, err}
	}()
	two := make(chan tuple, 1)
	go func() {
		s, err := OpenDevice(controller, lun)
		two <- tuple{s, err}
	}()

	// Give enough time for one or two to actually hit the resolve block
	time.Sleep(5 * time.Millisecond)

	// Unblock one of them to fail the resolve.
	wg.Done()

	oneResults := <-one
	twoResults := <-two
	if oneResults.e == nil {
		t.Fatal("expected first results failure got nil")
	}
	if oneResults.s != nil {
		t.Fatalf("expected first results nil scsi device got: %+v", oneResults.s)
	}

	if twoResults.e == nil {
		t.Fatal("expected two results failure got nil")
	}
	if twoResults.s != nil {
		t.Fatalf("expected two results nil scsi device got: %+v", twoResults.s)
	}

	if oneResults.e != twoResults.e {
		t.Fatalf("expected both failures to be identical one: %v, two: %v", oneResults.e, twoResults.e)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 call to ioutil.ReadDir got: %d", count)
	}

	// Verify there is no entry in the map
	_, loaded := scsiDevices.Load(scsiDevicesKey(controller, lun))
	if loaded {
		t.Fatal("expected map to not load based on key")
	}
}

// Test_OpenDevice_Second_Call_BothSuccess calls OpenDevice at the same time
// twice and verifies that when we simulate a success in the first call we get
// the same success in the second call with no additional call to resolve.
func Test_OpenDevice_Second_Call_BothSuccess(t *testing.T) {
	var count int32
	wg := sync.WaitGroup{}
	wg.Add(1)
	ioutilReadDir = func(dirname string) ([]os.FileInfo, error) {
		new := atomic.AddInt32(&count, 1)
		if new > 1 {
			t.Errorf("ioutil.ReadDir called %d times", new)
		}
		wg.Wait()
		return []os.FileInfo{
			&fakeFileInfo{
				name: "sda",
			},
		}, nil
	}

	var controller uint8 = 1
	var lun uint8 = 1

	defer forceCleanup(controller, lun)

	type tuple struct {
		s *Scsi
		e error
	}
	one := make(chan tuple, 1)
	go func() {
		s, err := OpenDevice(controller, lun)
		one <- tuple{s, err}
	}()
	two := make(chan tuple, 1)
	go func() {
		s, err := OpenDevice(controller, lun)
		two <- tuple{s, err}
	}()

	// Give enough time for one or two to actually hit the resolve block
	time.Sleep(5 * time.Millisecond)

	// Unblock one of them to fail the resolve.
	wg.Done()

	oneResults := <-one
	twoResults := <-two
	if oneResults.e != nil {
		t.Fatalf("expected first results not to fail got: %v", oneResults.e)
	}
	if oneResults.s == nil {
		t.Fatal("expected first results scsi device got nil")
	}

	if twoResults.e != nil {
		t.Fatalf("expected second results not to fail got: %v", twoResults.e)
	}
	if twoResults.s == nil {
		t.Fatal("expected second results scsi device got nil")
	}

	if oneResults.s != twoResults.s {
		t.Fatalf("expected both success scsi devices to be identical one: %+v, two: %+v", oneResults.s, twoResults.s)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 call to ioutil.ReadDir got: %d", count)
	}

	// Verify there is an entry in the map
	actualI, loaded := scsiDevices.Load(scsiDevicesKey(controller, lun))
	if !loaded {
		t.Fatal("expected map to load based on key")
	}
	actualS := actualI.(*Scsi)
	if actualS != oneResults.s {
		t.Fatalf("scsi device in map is not equal to one returned map: %+v, call: %+v", actualS, oneResults.s)
	}
}

func Test_Scsi_resolve_PreviousSuccess(t *testing.T) {
	ioutilReadDir = nil

	var controller uint8 = 1
	var lun uint8 = 1

	// Fake a previous resoution
	s := &Scsi{
		key:        scsiDevicesKey(controller, lun),
		controller: controller,
		lun:        lun,
		source:     "/dev/sdc",
	}

	// Simulate the same Scsi target having resolve called twice by the async
	// caller.
	err := s.resolve()
	if err != nil {
		t.Fatalf("expected success 2nd resolve got: %v", err)
	}
}

func Test_Scsi_resolve_PreviousFailure(t *testing.T) {
	ioutilReadDir = nil

	var controller uint8 = 1
	var lun uint8 = 1

	rerr := errors.New("resolve error")
	s := &Scsi{
		key:          scsiDevicesKey(controller, lun),
		controller:   controller,
		lun:          lun,
		resolveError: rerr,
	}

	// Simulate the same Scsi target having resolve called twice by the async
	// caller.
	err := s.resolve()
	if err != rerr {
		t.Fatalf("expected 2nd resolve err: %v, got: %v", rerr, err)
	}
}

func Test_Scsi_resolve_ReadDir_Failure(t *testing.T) {
	rerr := errors.New("intentional failure")
	ioutilReadDir = func(dirname string) ([]os.FileInfo, error) {
		// Anything but `os.ErrNotExit` should immediately exit
		return nil, rerr
	}

	var controller uint8 = 1
	var lun uint8 = 1

	s := &Scsi{
		key:        scsiDevicesKey(controller, lun),
		controller: controller,
		lun:        lun,
	}

	err := s.resolve()
	if err != rerr {
		t.Fatalf("expected ioutil.ReadDir failure: %v, got: %v", rerr, err)
	}
}

func Test_Scsi_resolve_Timeout_Failure(t *testing.T) {
	// Override timeout for a shorter test.
	orig := scsiDeviceLookupTimeout
	scsiDeviceLookupTimeout = 50 * time.Millisecond
	defer func() {
		scsiDeviceLookupTimeout = orig
	}()

	ioutilReadDir = func(dirname string) ([]os.FileInfo, error) {
		// `os.ErrNotExit` should retry up to timeout
		return nil, os.ErrNotExist
	}

	var controller uint8 = 1
	var lun uint8 = 1

	s := &Scsi{
		key:        scsiDevicesKey(controller, lun),
		controller: controller,
		lun:        lun,
	}

	err := s.resolve()
	if !os.IsNotExist(errors.Cause(err)) {
		t.Fatalf("expected resolve os.NotExitErr cause: got: %v", err)
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("expected error to contain \"timed out\", got: %v", err)
	}
}

func Test_Scsi_resolve_No_DeviceNames_Failure(t *testing.T) {
	ioutilReadDir = func(dirname string) ([]os.FileInfo, error) {
		// Simulate ../block exists but no entry under block
		return []os.FileInfo{}, nil
	}

	var controller uint8 = 1
	var lun uint8 = 1

	s := &Scsi{
		key:        scsiDevicesKey(controller, lun),
		controller: controller,
		lun:        lun,
	}

	err := s.resolve()
	if err == nil || !strings.Contains(err.Error(), "no matching device names") {
		t.Fatalf("expected error to contain \"no matching device names\", got: %v", err)
	}
}

func Test_Scsi_resolve_Success(t *testing.T) {
	ioutilReadDir = func(dirname string) ([]os.FileInfo, error) {
		return []os.FileInfo{
			&fakeFileInfo{
				name: "sdx",
			},
		}, nil
	}

	var controller uint8 = 1
	var lun uint8 = 1

	s := &Scsi{
		key:        scsiDevicesKey(controller, lun),
		controller: controller,
		lun:        lun,
	}

	err := s.resolve()
	if err != nil {
		t.Fatalf("expected no-error got: %v", err)
	}
	if s.source != "/dev/sdx" {
		t.Fatalf("expected s.source \"/dev/sdx\" got: %s", s.source)
	}
}

func Test_Scsi_resolve_TooMany_DeviceNames_Failure(t *testing.T) {
	ioutilReadDir = func(dirname string) ([]os.FileInfo, error) {
		// Simulate ../block exists but no entry under block
		return []os.FileInfo{
			&fakeFileInfo{
				name: "sdx",
			},
			&fakeFileInfo{
				name: "sdz",
			},
		}, nil
	}

	var controller uint8 = 1
	var lun uint8 = 1

	s := &Scsi{
		key:        scsiDevicesKey(controller, lun),
		controller: controller,
		lun:        lun,
	}

	err := s.resolve()
	if err == nil || !strings.Contains(err.Error(), "more than one block device") {
		t.Fatalf("expected error to contain \"more than one block device\", got: %v", err)
	}
}

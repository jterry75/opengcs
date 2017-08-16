package gcs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/Microsoft/opengcs/service/gcs/bridge"
	"github.com/Microsoft/opengcs/service/gcs/core/mockcore"
	"github.com/Microsoft/opengcs/service/gcs/oslayer"
	"github.com/Microsoft/opengcs/service/gcs/prot"
	"github.com/Microsoft/opengcs/service/gcs/transport"
	"github.com/sirupsen/logrus"
)

// TestResourceModificationSettings is a duplicate of
// prot.ResourceModificationSettings where each field is given explicitly
// rather than being inherited from embedded types. This allows the test code
// to easily construct a JSON string for the message without running into the
// Go JSON marshaller's inability to marshal field names which were inherited
// from multiple embedded types.
type TestResourceModificationSettings struct {
	ContainerPath     string
	Lun               uint8  `json:",omitempty"`
	CreateInUtilityVM bool   `json:",omitempty"`
	ReadOnly          bool   `json:",omitempty"`
	AttachOnly        bool   `json:",omitempty"`
	Port              uint32 `json:",omitempty"`
}

type testResponseWriter struct {
	header         *prot.MessageHeader
	response       interface{}
	err            error
	respWriteCount int
}

func (w *testResponseWriter) Header() *prot.MessageHeader {
	return w.header
}

func (w *testResponseWriter) Write(r interface{}) {
	w.response = r
	w.respWriteCount++
}

func (w *testResponseWriter) Error(err error) {
	w.err = err
	w.respWriteCount++
}

func createRequest(t *testing.T, id prot.MessageIdentifier, message interface{}) *bridge.Request {
	r := &bridge.Request{}

	bytes := make([]byte, 0)
	if message != nil {
		var err error
		bytes, err = json.Marshal(message)
		if err != nil {
			t.Fatalf("failed to marshal message for request: (%s)", err)
		}
	}
	hdr := &prot.MessageHeader{
		Type: id,
		Size: uint32(prot.MessageHeaderSize + len(bytes)),
		ID:   0,
	}

	r.Header = hdr
	r.Message = bytes
	return r
}

func createResponseWriter(r *bridge.Request) *testResponseWriter {
	hdr := &prot.MessageHeader{
		Type: prot.GetResponseIdentifier(r.Header.Type),
		ID:   r.Header.ID,
	}

	return &testResponseWriter{header: hdr}
}

func verifyResponseWriteCount(t *testing.T, rw *testResponseWriter) {
	if rw.respWriteCount != 1 {
		t.Fatalf("response was written (%d) times != 1", rw.respWriteCount)
	}
}

func verifyResponseError(t *testing.T, rw *testResponseWriter) {
	verifyResponseWriteCount(t, rw)
	if rw.err == nil {
		t.Fatal("response did not write an error")
	}
}

func verifyResponseJSONError(t *testing.T, rw *testResponseWriter) {
	verifyResponseError(t, rw)
	if !strings.Contains(rw.err.Error(), "failed to unmarshal JSON") {
		t.Fatal("response error was not a json marshal error")
	}
}

func verifyResponseSuccess(t *testing.T, rw *testResponseWriter) {
	verifyResponseWriteCount(t, rw)
	if rw.response == nil {
		t.Fatal("response was a success but no message was included")
	}
}

func Test_CreateContainer_InvalidJson_Failure(t *testing.T) {
	req := createRequest(t, prot.ComputeSystemCreateV1, nil)
	rw := createResponseWriter(req)

	gcsh := NewGcsHandler(nil, nil)
	gcsh.createContainer(rw, req)

	verifyResponseJSONError(t, rw)
}

func Test_CreateContainer_InvalidHostedJson_Failure(t *testing.T) {
	r := &prot.ContainerCreate{
		MessageBase: &prot.MessageBase{
			ContainerID: "test",
			ActivityID:  "act",
		},
	}

	req := createRequest(t, prot.ComputeSystemCreateV1, r)
	rw := createResponseWriter(req)

	gcsh := NewGcsHandler(nil, nil)
	gcsh.createContainer(rw, req)

	verifyResponseJSONError(t, rw)
}

func Test_CreateContainer_CoreCreateContainerFails_Failure(t *testing.T) {
	r := &prot.ContainerCreate{
		MessageBase: &prot.MessageBase{
			ContainerID: "test",
			ActivityID:  "act",
		},
		ContainerConfig: "{}", // Just unmarshal to defaults
	}

	req := createRequest(t, prot.ComputeSystemCreateV1, r)
	rw := createResponseWriter(req)

	mc := &mockcore.MockCore{Behavior: mockcore.Error}
	gcsh := NewGcsHandler(nil, mc)
	gcsh.createContainer(rw, req)

	verifyResponseError(t, rw)
}

func Test_CreateContainer_Success_WaitContainer_Failure(t *testing.T) {
	logrus.SetOutput(ioutil.Discard)

	r := &prot.ContainerCreate{
		MessageBase: &prot.MessageBase{
			ContainerID: "test",
			ActivityID:  "act",
		},
		ContainerConfig: "{}", // Just unmarshal to defaults
	}

	req := createRequest(t, prot.ComputeSystemCreateV1, r)
	rw := createResponseWriter(req)

	mc := &mockcore.MockCore{Behavior: mockcore.SingleSuccess}
	mc.WaitContainerWg.Add(1)

	gcsh := NewGcsHandler(nil, mc)
	gcsh.createContainer(rw, req)

	mc.WaitContainerWg.Wait()

	verifyResponseSuccess(t, rw)
}

/*
 * TODO: How to write this test. We need to have access to bridge.Bridge.responseChan
 * so that we can intercept the PublishNotificationCall.
func Test_CreateContainer_Success_WaitContainer_Success(t *testing.T) {
	r := &prot.ContainerCreate{
		MessageBase: &prot.MessageBase{
			ContainerID: "test",
			ActivityID:  "act",
		},
		ContainerConfig: "{}", // Just unmarshal to defaults
	}

	req := createRequest(t, prot.ComputeSystemCreateV1, r)
	rw := createResponseWriter(req)

	mc := &mockcore.MockCore{Behavior: mockcore.SingleSuccess}
	mc.WaitContainerWg.Add(1)

	publishWg := sync.WaitGroup{}
	publishWg.Add(1)
	f := func(response interface{}) {
		defer publishWg.Done()

		if response == nil {
			t.Fatal("publish response was nil")
			return
		}
		cn := response.(*prot.ContainerNotification)
		if cn.ContainerID != "test" {
			t.Fatal("publish response had invalid container ID")
		}
		if cn.ActivityID != "act" {
			t.Fatal("publish response had invalid activity ID")
		}
		if cn.Type != prot.NtUnexpectedExit {
			t.Fatal("publish response had invalid type")
		}
		if cn.Operation != prot.AoNone {
			t.Fatal("publish response had invalid operation")
		}
		if cn.Result != -1 {
			t.Fatal("publish response had invalid result")
		}
	}

	testBridge := bridgetest.NewTestBridge(f)
	gcsh := NewGcsHandler(testBridge, mc)
	gcsh.createContainer(rw, req)

	mc.WaitContainerWg.Wait()

	verifyResponseSuccess(t, rw)

	// Wait for the publish to take place on the exited notification.
	publishWg.Wait()
}
*/

func Test_ExecProcess_InvalidJson_Failure(t *testing.T) {
	req := createRequest(t, prot.ComputeSystemExecuteProcessV1, nil)
	rw := createResponseWriter(req)

	gcsh := NewGcsHandler(nil, nil)
	gcsh.execProcess(rw, req)

	verifyResponseJSONError(t, rw)
}

func Test_ExecProcess_InvalidProcessParameters_Failure(t *testing.T) {
	r := &prot.ContainerExecuteProcess{
		MessageBase: &prot.MessageBase{
			ContainerID: "test",
			ActivityID:  "act",
		},
		Settings: prot.ExecuteProcessSettings{
			ProcessParameters: "",
		},
	}

	req := createRequest(t, prot.ComputeSystemExecuteProcessV1, r)
	rw := createResponseWriter(req)

	gcsh := NewGcsHandler(nil, nil)
	gcsh.execProcess(rw, req)

	verifyResponseJSONError(t, rw)
}

type failureTransport struct {
	dialCount int
}

func (f *failureTransport) Dial(port uint32) (transport.Connection, error) {
	f.dialCount++
	return nil, fmt.Errorf("test failed to dial for port %d", port)
}

func Test_ExecProcess_ConnectFails_Failure(t *testing.T) {
	pp := prot.ProcessParameters{
		CreateStdInPipe:  true,
		CreateStdOutPipe: true,
		CreateStdErrPipe: true,
	}
	ppbytes, _ := json.Marshal(pp)
	r := &prot.ContainerExecuteProcess{
		MessageBase: &prot.MessageBase{
			ContainerID: "test",
			ActivityID:  "act",
		},
		Settings: prot.ExecuteProcessSettings{
			VsockStdioRelaySettings: prot.ExecuteProcessVsockStdioRelaySettings{
				StdIn:  1,
				StdOut: 2,
				StdErr: 3,
			},
			ProcessParameters: string(ppbytes),
		},
	}

	req := createRequest(t, prot.ComputeSystemCreateV1, r)
	rw := createResponseWriter(req)

	ft := &failureTransport{}
	b := &bridge.Bridge{
		Transport: ft,
	}

	gcsh := NewGcsHandler(b, nil)
	gcsh.execProcess(rw, req)

	verifyResponseError(t, rw)
	if ft.dialCount != 1 {
		t.Fatal("test dial count was not 1")
	}
}

func Test_ExecProcess_External_CoreFails_Failure(t *testing.T) {
	pp := prot.ProcessParameters{
		IsExternal: true,
	}
	ppbytes, _ := json.Marshal(pp)
	r := &prot.ContainerExecuteProcess{
		MessageBase: &prot.MessageBase{
			ContainerID: "test",
			ActivityID:  "act",
		},
		Settings: prot.ExecuteProcessSettings{
			ProcessParameters: string(ppbytes),
		},
	}

	req := createRequest(t, prot.ComputeSystemCreateV1, r)
	rw := createResponseWriter(req)

	ft := &failureTransport{} // Should not be called since we want no pipes
	b := &bridge.Bridge{
		Transport: ft,
	}

	mc := &mockcore.MockCore{Behavior: mockcore.Error}
	gcsh := NewGcsHandler(b, mc)
	gcsh.execProcess(rw, req)

	verifyResponseError(t, rw)
	if ft.dialCount != 0 {
		t.Fatal("test dial count was not 0")
	}
}

func Test_ExecProcess_External_CoreSucceeds_Success(t *testing.T) {
	pp := prot.ProcessParameters{
		IsExternal: true,
	}
	ppbytes, _ := json.Marshal(pp)
	r := &prot.ContainerExecuteProcess{
		MessageBase: &prot.MessageBase{
			ContainerID: "test",
			ActivityID:  "act",
		},
		Settings: prot.ExecuteProcessSettings{
			ProcessParameters: string(ppbytes),
		},
	}

	req := createRequest(t, prot.ComputeSystemCreateV1, r)
	rw := createResponseWriter(req)

	ft := &failureTransport{} // Should not be called since we want no pipes
	b := &bridge.Bridge{
		Transport: ft,
	}

	mc := &mockcore.MockCore{Behavior: mockcore.Success}
	gcsh := NewGcsHandler(b, mc)
	gcsh.execProcess(rw, req)

	verifyResponseSuccess(t, rw)
	if ft.dialCount != 0 {
		t.Fatal("test dial count was not 0")
	}
}

func Test_ExecProcess_Container_CoreFails_Failure(t *testing.T) {
	r := &prot.ContainerExecuteProcess{
		MessageBase: &prot.MessageBase{
			ContainerID: "test",
			ActivityID:  "act",
		},
		Settings: prot.ExecuteProcessSettings{
			ProcessParameters: "{}", // Default
		},
	}

	req := createRequest(t, prot.ComputeSystemCreateV1, r)
	rw := createResponseWriter(req)

	ft := &failureTransport{} // Should not be called since we want no pipes
	b := &bridge.Bridge{
		Transport: ft,
	}

	mc := &mockcore.MockCore{Behavior: mockcore.Error}
	gcsh := NewGcsHandler(b, mc)
	gcsh.execProcess(rw, req)

	verifyResponseError(t, rw)
	if ft.dialCount != 0 {
		t.Fatal("test dial count was not 0")
	}
}

func Test_ExecProcess_Container_CoreSucceeds_Success(t *testing.T) {
	r := &prot.ContainerExecuteProcess{
		MessageBase: &prot.MessageBase{
			ContainerID: "test",
			ActivityID:  "act",
		},
		Settings: prot.ExecuteProcessSettings{
			ProcessParameters: "{}", // Default
		},
	}

	req := createRequest(t, prot.ComputeSystemCreateV1, r)
	rw := createResponseWriter(req)

	ft := &failureTransport{} // Should not be called since we want no pipes
	b := &bridge.Bridge{
		Transport: ft,
	}

	mc := &mockcore.MockCore{Behavior: mockcore.Success}
	gcsh := NewGcsHandler(b, mc)
	gcsh.execProcess(rw, req)

	verifyResponseSuccess(t, rw)
	if ft.dialCount != 0 {
		t.Fatal("test dial count was not 0")
	}
}

func Test_KillContainer_InvalidJson_Failure(t *testing.T) {
	req := createRequest(t, prot.ComputeSystemShutdownForcedV1, nil)
	rw := createResponseWriter(req)

	gcsh := NewGcsHandler(nil, nil)
	gcsh.killContainer(rw, req)

	verifyResponseJSONError(t, rw)
}

func Test_KillContainer_CoreFails_Failure(t *testing.T) {
	r := &prot.MessageBase{
		ContainerID: "test",
		ActivityID:  "act",
	}

	req := createRequest(t, prot.ComputeSystemShutdownForcedV1, r)
	rw := createResponseWriter(req)

	mc := &mockcore.MockCore{Behavior: mockcore.Error}
	gcsh := NewGcsHandler(nil, mc)
	gcsh.killContainer(rw, req)

	verifyResponseError(t, rw)
}

func Test_KillContainer_CoreSucceeds_Success(t *testing.T) {
	r := &prot.MessageBase{
		ContainerID: "test",
		ActivityID:  "act",
	}

	req := createRequest(t, prot.ComputeSystemShutdownForcedV1, r)
	rw := createResponseWriter(req)

	mc := &mockcore.MockCore{Behavior: mockcore.Success}
	gcsh := NewGcsHandler(nil, mc)
	gcsh.killContainer(rw, req)

	verifyResponseSuccess(t, rw)
	if mc.LastSignalContainer.Signal != oslayer.SIGKILL {
		t.Fatal("last kill container signal was not SIGKILL")
	}
}

func Test_ShutdownContainer_InvalidJson_Failure(t *testing.T) {
	req := createRequest(t, prot.ComputeSystemShutdownForcedV1, nil)
	rw := createResponseWriter(req)

	gcsh := NewGcsHandler(nil, nil)
	gcsh.shutdownContainer(rw, req)

	verifyResponseJSONError(t, rw)
}

func Test_ShutdownContainer_CoreFails_Failure(t *testing.T) {
	r := &prot.MessageBase{
		ContainerID: "test",
		ActivityID:  "act",
	}

	req := createRequest(t, prot.ComputeSystemShutdownForcedV1, r)
	rw := createResponseWriter(req)

	mc := &mockcore.MockCore{Behavior: mockcore.Error}
	gcsh := NewGcsHandler(nil, mc)
	gcsh.shutdownContainer(rw, req)

	verifyResponseError(t, rw)
}

func Test_ShutdownContainer_CoreSucceeds_Success(t *testing.T) {
	r := &prot.MessageBase{
		ContainerID: "test",
		ActivityID:  "act",
	}

	req := createRequest(t, prot.ComputeSystemShutdownForcedV1, r)
	rw := createResponseWriter(req)

	mc := &mockcore.MockCore{Behavior: mockcore.Success}
	gcsh := NewGcsHandler(nil, mc)
	gcsh.shutdownContainer(rw, req)

	verifyResponseSuccess(t, rw)
	if mc.LastSignalContainer.Signal != oslayer.SIGTERM {
		t.Fatal("last shutdown container signal was not SIGTERM")
	}
}

func Test_SignalProcess_InvalidJson_Failure(t *testing.T) {
	req := createRequest(t, prot.ComputeSystemSignalProcessV1, nil)
	rw := createResponseWriter(req)

	gcsh := NewGcsHandler(nil, nil)
	gcsh.signalProcess(rw, req)

	verifyResponseJSONError(t, rw)
}

func Test_SignalProcess_CoreFails_Failure(t *testing.T) {
	r := &prot.ContainerSignalProcess{
		MessageBase: &prot.MessageBase{
			ContainerID: "test",
			ActivityID:  "act",
		},
		ProcessID: 20,
		Options: prot.SignalProcessOptions{
			Signal: 10,
		},
	}

	req := createRequest(t, prot.ComputeSystemSignalProcessV1, r)
	rw := createResponseWriter(req)

	mc := &mockcore.MockCore{Behavior: mockcore.Error}
	gcsh := NewGcsHandler(nil, mc)
	gcsh.signalProcess(rw, req)

	verifyResponseError(t, rw)
}

func Test_SignalProcess_CoreSucceeds_Success(t *testing.T) {
	r := &prot.ContainerSignalProcess{
		MessageBase: &prot.MessageBase{
			ContainerID: "test",
			ActivityID:  "act",
		},
		ProcessID: 20,
		Options: prot.SignalProcessOptions{
			Signal: 10,
		},
	}

	req := createRequest(t, prot.ComputeSystemSignalProcessV1, r)
	rw := createResponseWriter(req)

	mc := &mockcore.MockCore{Behavior: mockcore.Success}
	gcsh := NewGcsHandler(nil, mc)
	gcsh.signalProcess(rw, req)

	verifyResponseSuccess(t, rw)
	if uint32(mc.LastSignalProcess.Pid) != r.ProcessID {
		t.Fatal("last container signal process was not the same")
	}
}

//
// TODO: List Processes tests.
//

func Test_WaitOnProcess_InvalidJson_Failure(t *testing.T) {
	req := createRequest(t, prot.ComputeSystemWaitForProcessV1, nil)
	rw := createResponseWriter(req)

	gcsh := NewGcsHandler(nil, nil)
	gcsh.waitOnProcess(rw, req)

	verifyResponseJSONError(t, rw)
}

func Test_WaitOnProcess_CoreFails_Failure(t *testing.T) {
	r := &prot.ContainerWaitForProcess{
		MessageBase: &prot.MessageBase{
			ContainerID: "test",
			ActivityID:  "act",
		},
		ProcessID: 20,
	}

	req := createRequest(t, prot.ComputeSystemWaitForProcessV1, r)
	rw := createResponseWriter(req)

	mc := &mockcore.MockCore{Behavior: mockcore.Error}
	gcsh := NewGcsHandler(nil, mc)
	gcsh.waitOnProcess(rw, req)

	verifyResponseError(t, rw)
}

func Test_WaitOnProcess_CoreSucceeds_Success(t *testing.T) {
	r := &prot.ContainerWaitForProcess{
		MessageBase: &prot.MessageBase{
			ContainerID: "test",
			ActivityID:  "act",
		},
		ProcessID: 20,
	}

	req := createRequest(t, prot.ComputeSystemWaitForProcessV1, r)
	rw := createResponseWriter(req)

	mc := &mockcore.MockCore{Behavior: mockcore.Success}
	gcsh := NewGcsHandler(nil, mc)
	gcsh.waitOnProcess(rw, req)

	verifyResponseSuccess(t, rw)
	if uint32(mc.LastWaitProcess.Pid) != r.ProcessID {
		t.Fatal("last container wait on process pid was not the same")
	}
}

func Test_ResizeConsole_InvalidJson_Failure(t *testing.T) {
	req := createRequest(t, prot.ComputeSystemResizeConsoleV1, nil)
	rw := createResponseWriter(req)

	gcsh := NewGcsHandler(nil, nil)
	gcsh.resizeConsole(rw, req)

	verifyResponseJSONError(t, rw)
}

func Test_ResizeConsole_CoreFails_Failure(t *testing.T) {
	r := &prot.ContainerResizeConsole{
		MessageBase: &prot.MessageBase{
			ContainerID: "test",
			ActivityID:  "act",
		},
		ProcessID: 20,
		Width:     20,
		Height:    20,
	}

	req := createRequest(t, prot.ComputeSystemResizeConsoleV1, r)
	rw := createResponseWriter(req)

	mc := &mockcore.MockCore{Behavior: mockcore.Error}
	gcsh := NewGcsHandler(nil, mc)
	gcsh.resizeConsole(rw, req)

	verifyResponseError(t, rw)
}

func Test_ResizeConsole_CoreSucceeds_Success(t *testing.T) {
	r := &prot.ContainerResizeConsole{
		MessageBase: &prot.MessageBase{
			ContainerID: "test",
			ActivityID:  "act",
		},
		ProcessID: 20,
		Width:     640,
		Height:    480,
	}

	req := createRequest(t, prot.ComputeSystemResizeConsoleV1, r)
	rw := createResponseWriter(req)

	mc := &mockcore.MockCore{Behavior: mockcore.Success}
	gcsh := NewGcsHandler(nil, mc)
	gcsh.resizeConsole(rw, req)

	verifyResponseSuccess(t, rw)
	if uint32(mc.LastResizeConsole.Pid) != r.ProcessID {
		t.Fatal("last resize console process pid was not the same")
	}
	if mc.LastResizeConsole.Width != r.Width {
		t.Fatal("last resize console process width was not the same")
	}
	if mc.LastResizeConsole.Height != r.Height {
		t.Fatal("last resize console process height was not the same")
	}
}

func Test_ModifySettings_InvalidJson_Failure(t *testing.T) {
	req := createRequest(t, prot.ComputeSystemModifySettingsV1, nil)
	rw := createResponseWriter(req)

	gcsh := NewGcsHandler(nil, nil)
	gcsh.modifySettings(rw, req)

	verifyResponseJSONError(t, rw)
}

func Test_ModifySettings_VirtualDisk_InvalidSettingsJson_Failure(t *testing.T) {
	r := &prot.ContainerModifySettings{
		MessageBase: &prot.MessageBase{
			ContainerID: "test",
			ActivityID:  "act",
		},
		Request: prot.ResourceModificationRequestResponse{
			ResourceType: prot.PtMappedVirtualDisk,
		},
	}

	req := createRequest(t, prot.ComputeSystemModifySettingsV1, r)
	rw := createResponseWriter(req)

	gcsh := NewGcsHandler(nil, nil)
	gcsh.modifySettings(rw, req)

	verifyResponseJSONError(t, rw)
}

func Test_ModifySettings_MappedDirectory_InvalidSettingsJson_Failure(t *testing.T) {
	r := &prot.ContainerModifySettings{
		MessageBase: &prot.MessageBase{
			ContainerID: "test",
			ActivityID:  "act",
		},
		Request: prot.ResourceModificationRequestResponse{
			ResourceType: prot.PtMappedDirectory,
		},
	}

	req := createRequest(t, prot.ComputeSystemModifySettingsV1, r)
	rw := createResponseWriter(req)

	gcsh := NewGcsHandler(nil, nil)
	gcsh.modifySettings(rw, req)

	verifyResponseJSONError(t, rw)
}

func Test_ModifySettings_CoreFails_Failure(t *testing.T) {
	r := &prot.ContainerModifySettings{
		MessageBase: &prot.MessageBase{
			ContainerID: "test",
			ActivityID:  "act",
		},
		Request: prot.ResourceModificationRequestResponse{
			ResourceType: prot.PtMappedDirectory,
			Settings:     &prot.MappedDirectory{}, // Default values.
		},
	}

	req := createRequest(t, prot.ComputeSystemModifySettingsV1, r)
	rw := createResponseWriter(req)

	mc := &mockcore.MockCore{Behavior: mockcore.Error}
	gcsh := NewGcsHandler(nil, mc)
	gcsh.modifySettings(rw, req)

	verifyResponseError(t, rw)
}

func Test_ModifySettings_CoreSucceeds_Success(t *testing.T) {
	r := &prot.ContainerModifySettings{
		MessageBase: &prot.MessageBase{
			ContainerID: "test",
			ActivityID:  "act",
		},
		Request: prot.ResourceModificationRequestResponse{
			ResourceType: prot.PtMappedDirectory,
			Settings:     &prot.MappedDirectory{}, // Default values.
		},
	}

	req := createRequest(t, prot.ComputeSystemModifySettingsV1, r)
	rw := createResponseWriter(req)

	mc := &mockcore.MockCore{Behavior: mockcore.Success}
	gcsh := NewGcsHandler(nil, mc)
	gcsh.modifySettings(rw, req)

	verifyResponseSuccess(t, rw)
}

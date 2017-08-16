package gcs

import (
	"encoding/json"

	"github.com/Microsoft/opengcs/service/gcs/bridge"
	"github.com/Microsoft/opengcs/service/gcs/core"
	"github.com/Microsoft/opengcs/service/gcs/oslayer"
	"github.com/Microsoft/opengcs/service/gcs/prot"
	"github.com/Microsoft/opengcs/service/libs/commonutils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Handler is a struct used to listen, iterpret, and respond
// to bridge message events for a given message type. This is responsible
// for most of the low level json translation and then passes the message
// on to the gcs for actual dispatch/completion.
type Handler struct {
	bridge *bridge.Bridge
	core   core.Core
	mux    *bridge.Mux
}

// NewGcsHandler creates and assigns a multiplexer to handle
// all bridge based requests
func NewGcsHandler(b *bridge.Bridge, c core.Core) *Handler {
	h := &Handler{
		bridge: b,
		core:   c,
		mux:    bridge.NewBridgeMux(),
	}

	h.mux.HandleFunc(prot.ComputeSystemCreateV1, h.createContainer)
	h.mux.HandleFunc(prot.ComputeSystemExecuteProcessV1, h.execProcess)
	h.mux.HandleFunc(prot.ComputeSystemShutdownForcedV1, h.killContainer)
	h.mux.HandleFunc(prot.ComputeSystemShutdownGracefulV1, h.shutdownContainer)
	h.mux.HandleFunc(prot.ComputeSystemSignalProcessV1, h.signalProcess)
	h.mux.HandleFunc(prot.ComputeSystemGetPropertiesV1, h.listProcesses)
	h.mux.HandleFunc(prot.ComputeSystemWaitForProcessV1, h.waitOnProcess)
	h.mux.HandleFunc(prot.ComputeSystemResizeConsoleV1, h.resizeConsole)
	h.mux.HandleFunc(prot.ComputeSystemModifySettingsV1, h.modifySettings)

	return h
}

// ServeMsg forwards calls to the bridge multiplexer
func (h *Handler) ServeMsg(w bridge.ResponseWriter, r *bridge.Request) {
	h.mux.ServeMsg(w, r)
}

func (h *Handler) createContainer(w bridge.ResponseWriter, r *bridge.Request) {
	var request prot.ContainerCreate
	if err := commonutils.UnmarshalJSONWithHresult(r.Message, &request); err != nil {
		w.Error(errors.Wrapf(err, "failed to unmarshal JSON in message \"%s\"", r.Message))
		return
	}

	// The request contains a JSON string field which is equivalent to a
	// CreateContainerInfo struct.
	var settings prot.VMHostedContainerSettings
	if err := commonutils.UnmarshalJSONWithHresult([]byte(request.ContainerConfig), &settings); err != nil {
		w.Error(errors.Wrapf(err, "failed to unmarshal JSON for ContainerConfig \"%s\"", request.ContainerConfig))
		return
	}

	id := request.ContainerID
	if err := h.core.CreateContainer(id, settings); err != nil {
		w.Error(err)
		return
	}

	response := &prot.ContainerCreateResponse{
		MessageResponseBase: &prot.MessageResponseBase{
			ActivityID: request.ActivityID,
		},
		SelectedProtocolVersion: prot.PvV3,
	}
	w.Write(response)

	go func() {
		exitCode, err := h.core.WaitContainer(id)
		if err != nil {
			logrus.Error(err)
			return
		}
		notification := &prot.ContainerNotification{
			MessageBase: &prot.MessageBase{
				ContainerID: id,
				ActivityID:  request.ActivityID,
			},
			Type:       prot.NtUnexpectedExit, // TODO: Support different exit types.
			Operation:  prot.AoNone,
			Result:     int32(exitCode),
			ResultInfo: "",
		}
		h.bridge.PublishNotification(notification)
	}()
}

func (h *Handler) execProcess(w bridge.ResponseWriter, r *bridge.Request) {
	var request prot.ContainerExecuteProcess
	if err := commonutils.UnmarshalJSONWithHresult(r.Message, &request); err != nil {
		w.Error(errors.Wrapf(err, "failed to unmarshal JSON for message \"%s\"", r.Message))
		return
	}

	// The request contains a JSON string field which is equivalent to an
	// ExecuteProcessInfo struct.
	var params prot.ProcessParameters
	if err := commonutils.UnmarshalJSONWithHresult([]byte(request.Settings.ProcessParameters), &params); err != nil {
		w.Error(errors.Wrapf(err, "failed to unmarshal JSON for ProcessParameters \"%s\"", request.Settings.ProcessParameters))
		return
	}

	stdioSet, err := connectStdio(h.bridge.Transport, params, request.Settings.VsockStdioRelaySettings)
	if err != nil {
		w.Error(err)
		return
	}
	var pid int
	if params.IsExternal {
		pid, err = h.core.RunExternalProcess(params, stdioSet)
	} else {
		pid, err = h.core.ExecProcess(request.ContainerID, params, stdioSet)
	}

	if err != nil {
		stdioSet.Close() // stdioSet will be eventually closed by coreint on success
		w.Error(err)
		return
	}

	response := &prot.ContainerExecuteProcessResponse{
		MessageResponseBase: &prot.MessageResponseBase{
			ActivityID: request.ActivityID,
		},
		ProcessID: uint32(pid),
	}
	w.Write(response)
}

func (h *Handler) killContainer(w bridge.ResponseWriter, r *bridge.Request) {
	h.signalContainer(w, r, oslayer.SIGKILL)
}

func (h *Handler) shutdownContainer(w bridge.ResponseWriter, r *bridge.Request) {
	h.signalContainer(w, r, oslayer.SIGTERM)
}

// signalContainer is not a handler func. This is because the actual signal is
// implied based on the message type.
func (h *Handler) signalContainer(w bridge.ResponseWriter, r *bridge.Request, signal oslayer.Signal) {
	var request prot.MessageBase
	if err := commonutils.UnmarshalJSONWithHresult(r.Message, &request); err != nil {
		w.Error(errors.Wrapf(err, "failed to unmarshal JSON for message \"%s\"", r.Message))
		return
	}

	if err := h.core.SignalContainer(request.ContainerID, signal); err != nil {
		w.Error(err)
		return
	}

	response := &prot.MessageResponseBase{
		ActivityID: request.ActivityID,
	}
	w.Write(response)
}

func (h *Handler) signalProcess(w bridge.ResponseWriter, r *bridge.Request) {
	var request prot.ContainerSignalProcess
	if err := commonutils.UnmarshalJSONWithHresult(r.Message, &request); err != nil {
		w.Error(errors.Wrapf(err, "failed to unmarshal JSON for message \"%s\"", r.Message))
		return
	}

	if err := h.core.SignalProcess(int(request.ProcessID), request.Options); err != nil {
		w.Error(err)
		return
	}

	response := &prot.MessageResponseBase{
		ActivityID: request.ActivityID,
	}
	w.Write(response)
}

func (h *Handler) listProcesses(w bridge.ResponseWriter, r *bridge.Request) {
	var request prot.ContainerGetProperties
	if err := commonutils.UnmarshalJSONWithHresult(r.Message, &request); err != nil {
		w.Error(errors.Wrapf(err, "failed to unmarshal JSON for message \"%s\"", r.Message))
		return
	}
	id := request.ContainerID

	processes, err := h.core.ListProcesses(id)
	if err != nil {
		w.Error(err)
		return
	}

	processJSON, err := json.Marshal(processes)
	if err != nil {
		w.Error(errors.Wrapf(err, "failed to marshal processes into JSON: %v", processes))
		return
	}

	response := &prot.ContainerGetPropertiesResponse{
		MessageResponseBase: &prot.MessageResponseBase{
			ActivityID: request.ActivityID,
		},
		Properties: string(processJSON),
	}
	w.Write(response)
}

func (h *Handler) waitOnProcess(w bridge.ResponseWriter, r *bridge.Request) {
	var request prot.ContainerWaitForProcess
	if err := commonutils.UnmarshalJSONWithHresult(r.Message, &request); err != nil {
		w.Error(errors.Wrapf(err, "failed to unmarshal JSON for message \"%s\"", r.Message))
		return
	}

	exitCode, err := h.core.WaitProcess(int(request.ProcessID))
	if err != nil {
		w.Error(err)
		return
	}

	response := &prot.ContainerWaitForProcessResponse{
		MessageResponseBase: &prot.MessageResponseBase{
			ActivityID: request.ActivityID,
		},
		ExitCode: uint32(exitCode),
	}
	w.Write(response)
}

func (h *Handler) resizeConsole(w bridge.ResponseWriter, r *bridge.Request) {
	var request prot.ContainerResizeConsole
	if err := commonutils.UnmarshalJSONWithHresult(r.Message, &request); err != nil {
		w.Error(errors.Wrapf(err, "failed to unmarshal JSON for message \"%s\"", r.Message))
		return
	}

	if err := h.core.ResizeConsole(int(request.ProcessID), request.Height, request.Width); err != nil {
		w.Error(err)
		return
	}

	response := &prot.MessageResponseBase{
		ActivityID: request.ActivityID,
	}
	w.Write(response)
}

func (h *Handler) modifySettings(w bridge.ResponseWriter, r *bridge.Request) {
	request, err := prot.UnmarshalContainerModifySettings(r.Message)
	if err != nil {
		w.Error(errors.Wrapf(err, "failed to unmarshal JSON for message \"%s\"", r.Message))
		return
	}

	if err := h.core.ModifySettings(request.ContainerID, request.Request); err != nil {
		w.Error(err)
		return
	}

	response := &prot.MessageResponseBase{
		ActivityID: request.ActivityID,
	}
	w.Write(response)
}

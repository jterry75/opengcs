// Package bridge defines the bridge struct, which implements the control loop
// and functions of the GCS's bridge client.
package bridge

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"sync"

	gcserr "github.com/Microsoft/opengcs/service/gcs/errors"
	"github.com/Microsoft/opengcs/service/gcs/prot"
	"github.com/Microsoft/opengcs/service/gcs/transport"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// NotSupported represents the default handler logic for an unmatched
// request type sent from the bridge.
func NotSupported(w ResponseWriter, r *Request) {
	w.Error(gcserr.WrapHresult(errors.Errorf("bridge: function not supported, header type: 0x%x", r.Header.Type), gcserr.HrNotImpl))
}

// NotSupportedHandler creates a default HandlerFunc out of
// the NotSupported handler logic.
func NotSupportedHandler() Handler {
	return HandlerFunc(NotSupported)
}

// Handler responds to a bridge request.
type Handler interface {
	ServeMsg(ResponseWriter, *Request)
}

// HandlerFunc is an adapter to use functions as handlers.
type HandlerFunc func(ResponseWriter, *Request)

// ServeMsg calls f(w, r).
func (f HandlerFunc) ServeMsg(w ResponseWriter, r *Request) {
	f(w, r)
}

// Mux is a protocol multiplexer for request response pairs
// following the bridge protocol.
type Mux struct {
	mu sync.Mutex
	m  map[prot.MessageIdentifier]Handler
}

// NewBridgeMux creates a default bridge multiplexer.
func NewBridgeMux() *Mux {
	return &Mux{m: make(map[prot.MessageIdentifier]Handler)}
}

// Handle registers the handler for the given message id.
func (mux *Mux) Handle(id prot.MessageIdentifier, handler Handler) {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	if handler == nil {
		panic("bridge: nil handler")
	}

	if _, ok := mux.m[id]; ok {
		logrus.Infof("bridge: overwriting bridge handler for type: 0x%x", id)
	}

	mux.m[id] = handler
}

// HandleFunc registers the handler function for the given message id.
func (mux *Mux) HandleFunc(id prot.MessageIdentifier, handler func(ResponseWriter, *Request)) {
	if handler == nil {
		panic("bridge: nil handler func")
	}

	mux.Handle(id, HandlerFunc(handler))
}

// Handler returns the handler to use for the given request type.
func (mux *Mux) Handler(r *Request) Handler {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	if r == nil {
		panic("bridge: nil request to handler")
	}

	var h Handler
	var ok bool
	if h, ok = mux.m[r.Header.Type]; !ok {
		return NotSupportedHandler()
	}

	return h
}

// ServeMsg dispatches the request to the handler whose
// type matches the request type.
func (mux *Mux) ServeMsg(w ResponseWriter, r *Request) {
	h := mux.Handler(r)
	h.ServeMsg(w, r)
}

// Request is the bridge request that has been sent.
type Request struct {
	Header  *prot.MessageHeader
	Message []byte
}

// ResponseWriter is the dispatcher used to construct the Bridge response.
type ResponseWriter interface {
	// Header is the request header that was requested.
	Header() *prot.MessageHeader
	// Write a successful response message.
	Write(interface{})
	// Error writes the provided error as a response to the message.
	Error(error)
}

type bridgeResponse struct {
	header   *prot.MessageHeader
	response interface{}
}

type requestResponseWriter struct {
	header      *prot.MessageHeader
	respChan    chan bridgeResponse
	respWritten bool
}

func (w *requestResponseWriter) Header() *prot.MessageHeader {
	return w.header
}

func (w *requestResponseWriter) Write(r interface{}) {
	w.respChan <- bridgeResponse{header: w.header, response: r}
	w.respWritten = true
}

func (w *requestResponseWriter) Error(err error) {
	resp := &prot.MessageResponseBase{}
	setErrorForResponseBase(resp, err)
	w.Write(resp)
}

// Bridge defines the bridge client in the GCS. It acts in many
// ways analogous to go's `http` package and multiplexer.
//
// It has two fundamentally different dispatch options:
//
// 1. Request/Response where using the `Handler` a request
//    of a given type will be dispatched to the apprpriate handler
//    and an appropriate `ResponseWriter` will respond to exactly
//    that request that caused the dispatch.
//
// 2. `PublishNotification` where a notification that was not initiated
//    by a request from any client can be written to the bridge at any time
//    in any order.
type Bridge struct {
	// Transport is the transport interface used by the bridge.
	Transport transport.Transport

	// Handler to invoke when messages are received.
	Handler Handler

	// commandConn is the Connection the bridge receives commands (such as
	// ComputeSystemCreate) over.
	commandConn transport.Connection

	// responseChan is the response channel used for both request/response
	// and publish notification workflows.
	responseChan chan bridgeResponse

	// testing hook to close the bridge ListenAndServe() method.
	quitChan chan bool
}

// ListenAndServe connects to the bridge transport, listens for
// messages and dispatches the appropriate handlers to handle each
// event in an asynchronous manner.
func (b *Bridge) ListenAndServe() (conerr error) {
	const commandPort uint32 = 0x40000000

	var err error
	b.commandConn, err = b.Transport.Dial(commandPort)
	if err != nil {
		return errors.Wrap(err, "bridge: failed creating the command Connection")
	}
	logrus.Info("bridge: successfully connected to the HCS via HyperV_Socket\n")

	requestChan := make(chan *Request)
	requestErrChan := make(chan error)
	b.responseChan = make(chan bridgeResponse)
	responseErrChan := make(chan error)
	b.quitChan = make(chan bool)

	defer close(requestChan)
	defer close(requestErrChan)
	defer close(b.responseChan)
	defer close(responseErrChan)
	defer close(b.quitChan)

	// Receive bridge requests and schedule them to be processed.
	go func() {
		for {
			header := &prot.MessageHeader{}
			if err := binary.Read(b.commandConn, binary.LittleEndian, header); err != nil {
				requestErrChan <- errors.Wrap(err, "bridge: failed reading message header")
				continue
			}
			message := make([]byte, header.Size-prot.MessageHeaderSize)
			if _, err := io.ReadFull(b.commandConn, message); err != nil {
				requestErrChan <- errors.Wrap(err, "bridge: failed reading message payload")
				continue
			}
			logrus.Infof("bridge: read message '%s'\n", message)
			requestChan <- &Request{header, message}
		}
	}()
	// Process each bridge request async and create the response writer.
	go func() {
		for req := range requestChan {
			go func(r *Request) {
				wr := &requestResponseWriter{
					header: &prot.MessageHeader{
						Type: prot.GetResponseIdentifier(r.Header.Type),
						ID:   r.Header.ID,
					},
					respChan: b.responseChan,
				}
				b.Handler.ServeMsg(wr, r)
				if !wr.respWritten {
					logrus.Errorf("bridge: request: ID: 0x%x, Type: %d failed to write a response.\n", r.Header.ID, r.Header.Type)
				}
			}(req)
		}
	}()
	// Process each bridge response sync. This channel is for request/response and publish workflows.
	go func() {
		for resp := range b.responseChan {
			responseBytes, err := json.Marshal(resp.response)
			if err != nil {
				responseErrChan <- errors.Wrapf(err, "bridge: failed to marshal JSON for response \"%v\"", resp.response)
				continue
			}
			resp.header.Size = uint32(len(responseBytes) + prot.MessageHeaderSize)
			if err := binary.Write(b.commandConn, binary.LittleEndian, resp.header); err != nil {
				responseErrChan <- errors.Wrap(err, "bridge: failed writing message header")
				continue
			}

			if _, err := b.commandConn.Write(responseBytes); err != nil {
				responseErrChan <- errors.Wrap(err, "bridge: failed writing message payload")
				continue
			}
			logrus.Infof("bridge: response sent: '%s' to HCS\n", responseBytes)
		}
	}()
	// If we get any errors. We return from Listen and shutdown the bridge connection.
	select {
	case conerr = <-requestErrChan:
		break
	case conerr = <-responseErrChan:
		break
	case <-b.quitChan:
		break
	}
	return conerr
}

// PublishNotification writes a specific notification to the bridge.
func (b *Bridge) PublishNotification(n *prot.ContainerNotification) {
	if n == nil {
		panic("bridge: cannot publish nil notification")
	}

	resp := bridgeResponse{
		header: &prot.MessageHeader{
			Type: prot.ComputeSystemNotificationV1,
			ID:   0,
		},
		response: n,
	}
	b.responseChan <- resp
}

// setErrorForResponseBase modifies the passed-in MessageResponseBase to
// contain information pertaining to the given error.
func setErrorForResponseBase(response *prot.MessageResponseBase, errForResponse error) {
	errorMessage := errForResponse.Error()
	stackString := ""
	fileName := ""
	lineNumber := -1
	functionName := ""
	if stack := gcserr.BaseStackTrace(errForResponse); stack != nil {
		bottomFrame := stack[0]
		stackString = fmt.Sprintf("%+v", stack)
		fileName = fmt.Sprintf("%s", bottomFrame)
		lineNumberStr := fmt.Sprintf("%d", bottomFrame)
		var err error
		lineNumber, err = strconv.Atoi(lineNumberStr)
		if err != nil {
			logrus.Error(errors.Wrapf(err, "failed to parse \"%s\" as line number of error, using -1 instead", lineNumberStr))
			lineNumber = -1
		}
		functionName = fmt.Sprintf("%n", bottomFrame)
	}
	hresult, err := gcserr.GetHresult(errForResponse)
	if err != nil {
		// Default to using the generic failure HRESULT.
		hresult = gcserr.HrFail
	}
	response.Result = int32(hresult)
	newRecord := prot.ErrorRecord{
		Result:       int32(hresult),
		Message:      errorMessage,
		StackTrace:   stackString,
		ModuleName:   "gcs",
		FileName:     fileName,
		Line:         uint32(lineNumber),
		FunctionName: functionName,
	}
	response.ErrorRecords = append(response.ErrorRecords, newRecord)
}

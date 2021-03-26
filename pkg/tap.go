package pkg

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"github.com/linkerd/linkerd2/pkg/addr"
	"github.com/linkerd/linkerd2/pkg/protohttp"
	"github.com/linkerd/linkerd2/viz/tap/pkg"
	tapPb "github.com/linkerd/linkerd2/viz/tap/gen/tap"
	log "github.com/sirupsen/logrus"
)

type (
	Stream struct {
		Event   *tapPb.TapEvent
		ReqInit *tapPb.TapEvent_Http_RequestInit
		RspInit *tapPb.TapEvent_Http_ResponseInit
		RspEnd  *tapPb.TapEvent_Http_ResponseEnd
		TimestampMs uint64
	}

	streamID struct {
		src    string
		dst    string
		stream uint64
	}
)

func RecvEvents(tapByteStream *bufio.Reader, eventCh chan<- *tapPb.TapEvent, closing chan<- struct{}) {
	for {
		event := &tapPb.TapEvent{}
		err := protohttp.FromByteStreamToProtocolBuffers(tapByteStream, event)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Tap stream terminated")
			} else if !strings.HasSuffix(err.Error(), pkg.ErrClosedResponseBody) {
				fmt.Println(err.Error())
			}

			closing <- struct{}{}
			return
		}

		eventCh <- event
	}
}

func ProcessEvents(eventCh <-chan *tapPb.TapEvent, requestCh chan<- Stream, done <-chan struct{}) {
	outstandingRequests := make(map[streamID]Stream)

	for {
		select {
		case <-done:
			return
		case event := <-eventCh:
			id := streamID{
				src: addr.PublicAddressToString(event.GetSource()),
				dst: addr.PublicAddressToString(event.GetDestination()),
			}
			switch ev := event.GetHttp().GetEvent().(type) {
			case *tapPb.TapEvent_Http_RequestInit_:
				id.stream = ev.RequestInit.GetId().Stream
				outstandingRequests[id] = Stream{
					Event:   event,
					ReqInit: ev.RequestInit,
				}

			case *tapPb.TapEvent_Http_ResponseInit_:
				id.stream = ev.ResponseInit.GetId().Stream
				if req, ok := outstandingRequests[id]; ok {
					req.RspInit = ev.ResponseInit
					outstandingRequests[id] = req
				} else {
					log.Warnf("Got ResponseInit for unknown stream: %s", id)
				}

			case *tapPb.TapEvent_Http_ResponseEnd_:
				id.stream = ev.ResponseEnd.GetId().Stream
				if req, ok := outstandingRequests[id]; ok {
					req.RspEnd = ev.ResponseEnd
					requestCh <- req
				} else {
					log.Warnf("Got ResponseEnd for unknown stream: %s", id)
				}
			}
		}
	}
}
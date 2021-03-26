package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/adleong/tapshark/pkg"
	"github.com/gdamore/tcell/v2"
	"github.com/golang/protobuf/ptypes"
	"github.com/linkerd/linkerd2/pkg/addr"
	pkgcmd "github.com/linkerd/linkerd2/pkg/cmd"
	"github.com/linkerd/linkerd2/pkg/healthcheck"
	"github.com/linkerd/linkerd2/pkg/k8s"
	vizpkg "github.com/linkerd/linkerd2/viz/pkg"
	"github.com/linkerd/linkerd2/viz/pkg/api"
	tapPb "github.com/linkerd/linkerd2/viz/tap/gen/tap"
	tapPkg "github.com/linkerd/linkerd2/viz/tap/pkg"
	"github.com/rivo/tview"
	"github.com/spf13/cobra"
)

const	defaultLinkerdNamespace = "linkerd"

type (
	eventLog struct {
		app     *tview.Application
		table   *tview.Table
		details *tview.TextView
		events  []pkg.Stream
	}

	options struct {
		apiAddr               string // An empty value means "use the Kubernetes configuration"
		controlPlaneNamespace string
		kubeconfigPath        string
		kubeContext           string
		impersonate           string
		impersonateGroup      []string

		namespace     string
		toResource    string
		toNamespace   string
		maxRps        float32
		scheme        string
		method        string
		authority     string
		path          string
		labelSelector string
	}
)

// NewCmdTapShark creates a new cobra command `tap` for tap functionality
func NewCmdTapShark() *cobra.Command {
	options := options{}

	cmd := &cobra.Command{
		Use:   "tapshark [flags] (RESOURCE)",
		Short: "Listen to a traffic stream",
		Long: `Listen to a traffic stream.

  The RESOURCE argument specifies the target resource(s) to tap:
  (TYPE [NAME] | TYPE/NAME)

  Examples:
  * cronjob/my-cronjob
  * deploy
  * deploy/my-deploy
  * deploy my-deploy
  * ds/my-daemonset
  * job/my-job
  * ns/my-ns
  * rs
  * rs/my-replicaset
  * sts
  * sts/my-statefulset

  Valid resource types include:
  * cronjobs
  * daemonsets
  * deployments
  * jobs
  * namespaces
  * pods
  * replicasets
  * replicationcontrollers
  * statefulsets
  * services (only supported as a --to resource)`,
		Example: `  # tap the web deployment in the default namespace
  linkerd viz tapshark deploy/web

  # tap the web-dlbvj pod in the default namespace
  linkerd viz tapshark pod/web-dlbvj

  # tap the test namespace, filter by request to prod namespace
  linkerd viz tapshark ns/test --to ns/prod`,
		Args:      cobra.RangeArgs(1, 2),
		ValidArgs: vizpkg.ValidTargets,
		RunE: func(cmd *cobra.Command, args []string) error {
			if options.namespace == "" {
				options.namespace = pkgcmd.GetDefaultNamespace(options.kubeconfigPath, options.kubeContext)
			}

			api.CheckClientOrExit(healthcheck.Options{
				ControlPlaneNamespace: options.controlPlaneNamespace,
				KubeConfig:            options.kubeconfigPath,
				Impersonate:           options.impersonate,
				ImpersonateGroup:      options.impersonateGroup,
				KubeContext:           options.kubeContext,
				APIAddr:               options.apiAddr,
			})

			requestParams := tapPkg.TapRequestParams{
				Resource:      strings.Join(args, "/"),
				Namespace:     options.namespace,
				ToResource:    options.toResource,
				ToNamespace:   options.toNamespace,
				MaxRps:        options.maxRps,
				Scheme:        options.scheme,
				Method:        options.method,
				Authority:     options.authority,
				Path:          options.path,
				Extract:       true,
				LabelSelector: options.labelSelector,
			}

			req, err := tapPkg.BuildTapByResourceRequest(requestParams)
			if err != nil {
				fmt.Fprint(os.Stderr, err.Error())
				os.Exit(1)
			}

			k8sAPI, err := k8s.NewAPI(options.kubeconfigPath, options.kubeContext, options.impersonate, options.impersonateGroup, 0)
			if err != nil {
				fmt.Fprint(os.Stderr, err.Error())
				os.Exit(1)
			}

			headers := []string{"TIME", pad("FROM"), pad("POD"), pad("TO"), pad("VERB"), pad("PATH"), pad("STATUS"), "LATENCY"}

			table := tview.NewTable().SetFixed(1, 0).SetSelectable(true, false)
			for i, header := range headers {
				cell := tview.NewTableCell(header)
				cell.SetAttributes(tcell.AttrBold)
				table.SetCell(0, i, cell)
			}

			done := make(chan struct{})

			details := tview.NewTextView().SetDynamicColors(true)

			grid := tview.NewGrid().SetSize(2, 1, -1, -1).
				AddItem(table, 0, 0, 1, 1, 0, 0, true).
				AddItem(details, 1, 0, 1, 1, 0, 0, false).
				SetBorders(true)
			grid.SetTitle(strings.Join(os.Args, " "))

			app := tview.NewApplication().SetRoot(grid, true)
			app.SetInputCapture(
				func(event *tcell.EventKey) *tcell.EventKey {
					if event.Key() == tcell.KeyTAB {
						if table.HasFocus() {
							app.SetFocus(details)
						} else {
							app.SetFocus(table)
						}
						return nil
					}
					return event
				})

			eventLog := &eventLog{
				app:     app,
				details: details,
				table:   table,
				events:  []pkg.Stream{},
			}

			table.SetSelectedFunc(eventLog.selectionChanged)

			go eventLog.processTapEvents(cmd.Context(), k8sAPI, req, done)

			if err := app.Run(); err != nil {
				panic(err)
			}

			done <- struct{}{}

			return nil
		},
	}

	cmd.Flags().StringVarP(&options.controlPlaneNamespace, "linkerd-namespace", "L", defaultLinkerdNamespace, "Namespace in which Linkerd is installed")
	cmd.Flags().StringVar(&options.kubeconfigPath, "kubeconfig", "", "Path to the kubeconfig file to use for CLI requests")
	cmd.Flags().StringVar(&options.kubeContext, "context", "", "Name of the kubeconfig context to use")
	cmd.Flags().StringVar(&options.impersonate, "as", "", "Username to impersonate for Kubernetes operations")
	cmd.Flags().StringArrayVar(&options.impersonateGroup, "as-group", []string{}, "Group to impersonate for Kubernetes operations")
	cmd.Flags().StringVar(&options.apiAddr, "api-addr", "", "Override kubeconfig and communicate directly with the control plane at host:port (mostly for testing)")
	cmd.Flags().StringVarP(&options.namespace, "namespace", "n", options.namespace,
		"Namespace of the specified resource")
	cmd.Flags().StringVar(&options.toResource, "to", options.toResource,
		"Display requests to this resource")
	cmd.Flags().StringVar(&options.toNamespace, "to-namespace", options.toNamespace,
		"Sets the namespace used to lookup the \"--to\" resource; by default the current \"--namespace\" is used")
	cmd.Flags().Float32Var(&options.maxRps, "max-rps", options.maxRps,
		"Maximum requests per second to pkg.")
	cmd.Flags().StringVar(&options.scheme, "scheme", options.scheme,
		"Display requests with this scheme")
	cmd.Flags().StringVar(&options.method, "method", options.method,
		"Display requests with this HTTP method")
	cmd.Flags().StringVar(&options.authority, "authority", options.authority,
		"Display requests with this :authority")
	cmd.Flags().StringVar(&options.path, "path", options.path,
		"Display requests with paths that start with this prefix")
	cmd.Flags().StringVarP(&options.labelSelector, "selector", "l", options.labelSelector,
		"Selector (label query) to filter on, supports '=', '==', and '!='")

	return cmd
}

func (el *eventLog) processTapEvents(ctx context.Context, k8sAPI *k8s.KubernetesAPI, req *tapPb.TapByResourceRequest, done <-chan struct{}) {
	reader, body, err := tapPkg.Reader(ctx, k8sAPI, req)
	if err != nil {
		fmt.Fprint(os.Stderr, err.Error())
		return
	}
	defer body.Close()

	eventCh := make(chan *tapPb.TapEvent)
	requestCh := make(chan pkg.Stream, 100)

	closing := make(chan struct{}, 1)

	go pkg.RecvEvents(reader, eventCh, closing)
	go pkg.ProcessEvents(eventCh, requestCh, done)

	go func() {
		<-closing
	}()

	start := time.Now()

	for {
		select {
		case <-done:
			return
		case req := <-requestCh:

			delta := time.Since(start)
			req.TimestampMs = uint64(delta.Milliseconds())

			el.events = append(el.events, req)
			row := len(el.events)

			timestamp := fmt.Sprintf("%.3f", float64(req.TimestampMs)/1000.0)
			from, pod, to := fromPodTo(req)
			verb := req.ReqInit.GetMethod().GetRegistered().String()
			path := req.ReqInit.GetPath()
			status := fmt.Sprintf("%d", req.RspInit.GetHttpStatus())
			latency := latency(req)

			el.app.QueueUpdateDraw(func() {
				el.table.SetCellSimple(row, 0, timestamp)
				el.table.SetCellSimple(row, 1, pad(from))
				el.table.SetCellSimple(row, 2, pad(pod))
				el.table.SetCellSimple(row, 3, pad(to))
				el.table.SetCellSimple(row, 4, pad(verb))
				el.table.SetCellSimple(row, 5, pad(path))
				el.table.SetCellSimple(row, 6, pad(status))
				el.table.SetCellSimple(row, 7, latency)
			})
		}
	}

}

func (el *eventLog) selectionChanged(row, column int) {
	if row == 0 {
		el.details.Clear()
		return
	}
	req := el.events[row-1]
	from, pod, to := fromPodTo(req)
	el.details.Clear()

	fieldTemplate := "[::b]%s:[-:-:-] %s\n"

	fmt.Fprintf(el.details, fieldTemplate, "Pod", pod)
	if from != "" {
		fmt.Fprintf(el.details, fieldTemplate, "From", from)
	}
	if to != "" {
		fmt.Fprintf(el.details, fieldTemplate, "To", to)
	}
	fmt.Fprintln(el.details)

	fmt.Fprintf(el.details, fieldTemplate, "Source", addr.PublicAddressToString(req.Event.GetSource()))
	fmt.Fprintf(el.details, fieldTemplate, "Source Metadata", "")
	for k, v := range req.Event.GetSourceMeta().GetLabels() {
		fmt.Fprintf(el.details, "\t%s: %s\n", k, v)
	}
	fmt.Fprintf(el.details, fieldTemplate, "Destination", addr.PublicAddressToString(req.Event.GetDestination()))
	fmt.Fprintf(el.details, fieldTemplate, "Destination Metadata", "")
	for k, v := range req.Event.GetDestinationMeta().GetLabels() {
		fmt.Fprintf(el.details, "\t%s: %s\n", k, v)
	}
	fmt.Fprintln(el.details)

	if len(req.Event.GetRouteMeta().GetLabels()) > 0 {
		fmt.Fprintf(el.details, fieldTemplate, "Route Metadata", "")
		for k, v := range req.Event.GetRouteMeta().GetLabels() {
			fmt.Fprintf(el.details, "\t%s: %s\n", k, v)
		}
		fmt.Fprintln(el.details)
	}

	fmt.Fprintf(el.details, fieldTemplate, "Scheme", req.ReqInit.GetScheme().GetRegistered().String())
	fmt.Fprintf(el.details, fieldTemplate, "Verb", req.ReqInit.GetMethod().GetRegistered().String())
	fmt.Fprintf(el.details, fieldTemplate, "Path", req.ReqInit.GetPath())
	fmt.Fprintf(el.details, fieldTemplate, "Authority", req.ReqInit.GetAuthority())
	fmt.Fprintf(el.details, fieldTemplate, "Request Headers", "")
	for _, header := range req.ReqInit.GetHeaders().GetHeaders() {
		fmt.Fprintf(el.details, "\t%s: %s\n", header.GetName(), header.GetValueStr())
	}
	fmt.Fprintf(el.details, fieldTemplate, "Latency", latency(req))
	fmt.Fprintf(el.details, fieldTemplate, "Status", fmt.Sprintf("%d", req.RspInit.GetHttpStatus()))

	var duration string
	d, err := ptypes.Duration(req.RspEnd.GetSinceResponseInit())
	if err == nil {
		duration = d.String()
	}

	fmt.Fprintf(el.details, fieldTemplate, "Duration", duration)
	fmt.Fprintf(el.details, fieldTemplate, "Response Headers", "")
	for _, header := range req.RspInit.GetHeaders().GetHeaders() {
		fmt.Fprintf(el.details, "\t%s: %s\n", header.GetName(), header.GetValueStr())
	}
	fmt.Fprintf(el.details, fieldTemplate, "Response Trailers", "")
	for _, header := range req.RspEnd.Trailers.GetHeaders() {
		fmt.Fprintf(el.details, "\t%s: %s\n", header.GetName(), header.GetValueStr())
	}
	el.details.ScrollToBeginning()
}

func pad(s string) string {
	return fmt.Sprintf(" %s ", s)
}

func fromPodTo(req pkg.Stream) (string, string, string) {
	source := stripPort(addr.PublicAddressToString(req.Event.GetSource()))
	if pod := req.Event.SourceMeta.Labels["pod"]; pod != "" {
		source = pod
	}
	destination := stripPort(addr.PublicAddressToString(req.Event.GetDestination()))
	if pod := req.Event.DestinationMeta.Labels["pod"]; pod != "" {
		destination = pod
	}
	var from, pod, to string
	if req.Event.GetProxyDirection() == tapPb.TapEvent_INBOUND {
		from = source
		pod = destination
	} else if req.Event.GetProxyDirection() == tapPb.TapEvent_OUTBOUND {
		pod = source
		to = destination
	}
	return from, pod, to
}

func latency(req pkg.Stream) string {
	latency, err := ptypes.Duration(req.RspEnd.GetSinceRequestInit())
	if err != nil {
		return ""
	}
	return latency.String()
}

func stripPort(address string) string {
	return strings.Split(address, ":")[0]
}

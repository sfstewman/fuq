package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/sfstewman/fuq"
	"github.com/sfstewman/fuq/srv"
	"os"
	"sort"
	"strconv"
)

type Command struct {
	Description string
	Run         func(fuq.Config, []string) error
}

const DefaultClientString = "fuqcli-alpha"

var (
	ClientString  string = DefaultClientString
	sysConfigFile string
	srvConfigFile string
	config        fuq.Config
	subCmds       map[string]Command
	globalSet     *flag.FlagSet
)

func init() {
	globalSet = flag.NewFlagSet("global", flag.ContinueOnError)
	globalSet.StringVar(&srvConfigFile, "srv", "", "server config file")
	globalSet.StringVar(&sysConfigFile, "cfg", "", "system config file")
	globalSet.StringVar(&config.Foreman, "host", config.Foreman, "host of the queue foreman")
	globalSet.IntVar(&config.Port, "port", config.Port, "port of the queue foreman")
	globalSet.StringVar(&config.Auth, "auth", config.Auth, "authorization key of the queue foreman")
	globalSet.StringVar(&config.KeyFile, "key", "", "path to TLS key file")
	globalSet.StringVar(&config.CertFile, "cert", "", "path to TLS cert file")
}

func callEndpoint(cfg fuq.Config, dest string, msg, ret interface{}) error {
	ep, err := fuq.NewEndpoint(cfg)
	if err != nil {
		return err
	}

	env := srv.ClientRequestEnvelope{
		Auth: fuq.Client{Password: cfg.Auth, Client: ClientString},
		Msg:  msg,
	}

	return ep.CallEndpoint(dest, &env, &ret)
}

func queueCmd(cfg fuq.Config, args []string) error {
	var err error
	job := fuq.JobDescription{
		NumTasks: 1,
	}

	if job.WorkingDir, err = os.Getwd(); err != nil {
		return fmt.Errorf("Error looking up working directory: %v", err)
	}

	outputJSON := false
	jobFile := ""

	queueFlags := flag.NewFlagSet("queue", flag.ContinueOnError)
	queueFlags.IntVar(&job.NumTasks, "n", job.NumTasks, "number of tasks")
	queueFlags.StringVar(&job.WorkingDir, "dir", job.WorkingDir, "working directory")
	queueFlags.StringVar(&job.LoggingDir, "log", job.WorkingDir, "logging directory")
	queueFlags.BoolVar(&outputJSON, "j", outputJSON, "output json")
	queueFlags.StringVar(&jobFile, "f", jobFile, "read job description from a file")

	usage := Usage{"queue", "job_name job_command", queueFlags}

	if err := queueFlags.Parse(args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "error parsing queue flags: %v", err)
		usage.ShowAndExit()
	}

	pv, err := fuq.SetupPaths()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error setting up paths: %v", err)
		os.Exit(1)
	}

	if jobFile != "" {
		f, err := os.Open(jobFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error opening job file '%s': %v",
				jobFile, err)
			os.Exit(1)
		}
		defer f.Close()

		jobInFile, err := fuq.ReadJobFile(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error reading job file '%s':\n%v",
				jobFile, err)
			os.Exit(1) // XXX: close file first?
		}

		job = jobInFile
		// reparse to allow them to override any settings
		if err := queueFlags.Parse(args[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "error parsing queue flags: %v", err)
			usage.ShowAndExit()
		}
	}

	if jobFile == "" && queueFlags.NArg() != 2 {
		fmt.Fprintf(os.Stderr, "expected two queue arguments (found %d)\n", queueFlags.NArg())
		usage.ShowAndExit()
	}

	qArgs := queueFlags.Args()
	if len(qArgs) > 0 {
		job.Name = qArgs[0]
		job.Command = qArgs[1]
	}

	if err := job.ExpandPaths(pv); err != nil {
		fmt.Fprintf(os.Stderr, "error in job: %v", err)
		usage.ShowAndExit()
	}

	if err := job.CheckPaths(); err != nil {
		fmt.Fprintf(os.Stderr, "error in job: %v", err)
		usage.ShowAndExit()
	}

	cmd := job.Command
	if job.Command, err = pv.ExpandPath(cmd); err != nil {
		fmt.Fprintf(os.Stderr, "error expanding command '%s': %v", cmd, err)
		os.Exit(1)
	}

	if job.Command == "" {
		fmt.Fprintf(os.Stderr, "invalid command %s", cmd)
		usage.ShowAndExit()
	}

	if !outputJSON {
		fmt.Printf("Queueing job '%s'\n", job.Name)
		fmt.Printf("Command    : %s\n", job.Command)
		fmt.Printf("Working dir: %s\n", job.WorkingDir)
		fmt.Printf("Logging dir: %s\n", job.LoggingDir)
		fmt.Printf("Num Tasks  : %d\n", job.NumTasks)
	}

	ret := fuq.NewJobResponse{}
	if err := callEndpoint(cfg, "client/job/new", &job, &ret); err != nil {
		return fmt.Errorf("Error encountered queuing job: %v", err)
	}

	if !outputJSON {
		fmt.Printf("job_id     : %d\n", ret.JobId)
	} else {
		enc := json.NewEncoder(os.Stdout)
		job.JobId = ret.JobId
		if err := enc.Encode(&job); err != nil {
			return fmt.Errorf("Error encoding job: %v", err)
		}
	}

	return nil
}

type SortByJobId []fuq.JobTaskStatus

func (js SortByJobId) Len() int           { return len(js) }
func (js SortByJobId) Less(i, j int) bool { return js[i].Description.JobId < js[j].Description.JobId }
func (js SortByJobId) Swap(i, j int)      { js[i], js[j] = js[j], js[i] }

func statusCmd(cfg fuq.Config, args []string) error {
	req := fuq.ClientJobListReq{}
	longListing := false
	outputJSON := false

	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	flags.StringVar(&req.Name, "name", req.Name, "job name")
	flags.StringVar(&req.Status, "status", req.Status,
		"job status: running, waiting, paused, or finished")
	flags.BoolVar(&longListing, "l", longListing, "show long listing")
	flags.BoolVar(&outputJSON, "j", outputJSON, "output json response")

	usage := Usage{"status", "", flags}

	if err := flags.Parse(args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "error parsing %s flags: %v", args[0], err)
		usage.ShowAndExit()
	}

	if flags.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "status expects no arguments (found %d)\n", flags.NArg())
		usage.ShowAndExit()
	}

	// ret := []fuq.JobDescription{}
	ret := []fuq.JobTaskStatus{}
	if err := callEndpoint(cfg, "client/job/list", &req, &ret); err != nil {
		return fmt.Errorf("Error encountered queuing job: %v", err)
	}

	if outputJSON {
		enc := json.NewEncoder(os.Stdout)
		if err := enc.Encode(&ret); err != nil {
			return fmt.Errorf("Error encoding job: %v", err)
		}
		return nil
	}

	sort.Sort(SortByJobId(ret))

	if longListing {
		for _, j := range ret {
			jd := j.Description
			fmt.Printf("Job %d\n", jd.JobId)
			fmt.Printf("Name        : %s\n", jd.Name)
			fmt.Printf("Command     : %s\n", jd.Command)
			fmt.Printf("Working dir : %s\n", jd.WorkingDir)
			fmt.Printf("Logging dir : %s\n", jd.LoggingDir)
			fmt.Printf("Num Tasks   : %d\n", jd.NumTasks)
			fmt.Printf("    Finished: %d\n", j.TasksFinished)
			fmt.Printf("    Errors  : %d\n", j.TasksWithErrors)
			fmt.Printf("Running     :")
			for i, t := range j.TasksRunning {
				if (i+1)%10 == 0 {
					fmt.Printf("\nRunning     :")
				}
				fmt.Printf(" %d", t)
			}
			fmt.Printf("\n\n")
		}
	} else {
		fmt.Printf("%-4s\t%-30.30s\t%-12s\t%-8s\t%-8s\t%-8s\n",
			"Job", "Name", "Status", "Tasks", "Running", "Done")
		for _, j := range ret {
			jd := j.Description
			fmt.Printf("%4d\t%-30.30s\t%-12s\t%8d\t%8d\t%8d\n",
				jd.JobId, jd.Name, jd.Status, jd.NumTasks,
				len(j.TasksRunning), j.TasksFinished)
		}
	}

	return nil
}

func changeStateCmd(cfg fuq.Config, args []string) error {
	req := fuq.ClientStateChangeReq{}
	outputJSON := false

	flags := flag.NewFlagSet("changeState", flag.ContinueOnError)
	flags.BoolVar(&outputJSON, "j", outputJSON, "output json response")

	flags.Parse(args[1:])

	usage := Usage{args[0], "id1 id2 ...", flags}

	if err := flags.Parse(args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "error parsing %s flags: %v", args[0], err)
		usage.ShowAndExit()
	}

	if flags.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "%s expects at least one argument (found %d)\n",
			args[0], flags.NArg())
		usage.ShowAndExit()
	}

	cmdArgs := flags.Args()

	switch args[0] {
	case "pause", "hold":
		req.Action = "hold"
	case "unpause", "release":
		req.Action = "release"
	case "cancel":
		req.Action = "cancel"
	}

	req.JobIds = make([]fuq.JobId, 0, len(cmdArgs))
	/* parse arguments */
	for _, idArg := range cmdArgs {
		id, err := strconv.ParseUint(idArg, 0, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid job id: %s\n", idArg)
		}
		req.JobIds = append(req.JobIds, fuq.JobId(id))
	}

	ret := []fuq.JobStateChangeResponse{}
	if err := callEndpoint(cfg, "client/job/state", &req, &ret); err != nil {
		return fmt.Errorf("Error encountered changing state of jobs %v: %v",
			req.JobIds, err)
	}

	if outputJSON {
		enc := json.NewEncoder(os.Stdout)
		if err := enc.Encode(&ret); err != nil {
			return fmt.Errorf("Error encoding job: %v", err)
		}
		return nil
	}

	for _, ch := range ret {
		fmt.Printf("%6d\t%-12s -> %-12s\n",
			ch.JobId, ch.PrevStatus, ch.NewStatus)
	}

	return nil
}

func workerListCmd(cfg fuq.Config, args []string) error {
	outputJSON := false

	flags := flag.NewFlagSet("workerList", flag.ContinueOnError)
	flags.BoolVar(&outputJSON, "j", outputJSON, "output json response")

	flags.Parse(args[1:])

	usage := Usage{args[0], "id1 id2 ...", flags}

	if err := flags.Parse(args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "error parsing %s flags: %v", args[0], err)
		usage.ShowAndExit()
	}

	if flags.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "%s expects no arguments (found %d)\n",
			args[0], flags.NArg())
		usage.ShowAndExit()
	}

	ret := []fuq.NodeInfo{}
	if err := callEndpoint(cfg, "client/nodes/list", nil, &ret); err != nil {
		return fmt.Errorf("Error encountered listing worker nodes: %v", err)
	}

	if outputJSON {
		enc := json.NewEncoder(os.Stdout)
		if err := enc.Encode(&ret); err != nil {
			return fmt.Errorf("Error encoding node list: %v", err)
		}
		return nil
	}

	for i, ni := range ret {
		fmt.Printf("%4d\t%-12s\t%-24s\t%3d",
			i+1, ni.Node, ni.UniqName, ni.NumProc)
		if len(ni.Tags) > 0 {
			fmt.Printf("\t%s", ni.Tags[0])
			for _, t := range ni.Tags[1:] {
				fmt.Printf(", %s", t)
			}
		}
		fmt.Printf("\n")
	}

	return nil
}

func workerStopCmd(cfg fuq.Config, args []string) error {
	var (
		tag string
		all bool
	)

	flags := flag.NewFlagSet("workerStop", flag.ContinueOnError)
	flags.StringVar(&tag, "t", "", "stops workers with tag")
	flags.BoolVar(&all, "a", false, "stops all workers")

	flags.Parse(args[1:])

	usage := Usage{args[0], "[node_name1 node_name2 ...]", flags}

	if err := flags.Parse(args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "error parsing %s flags: %v", args[0], err)
		usage.ShowAndExit()
	}

	if !all && tag == "" && flags.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "%s requires a node selection (all, tag, or names)\n",
			args[0])
		usage.ShowAndExit()
	}

	nodes := []fuq.NodeInfo{}
	if err := callEndpoint(cfg, "client/nodes/list", nil, &nodes); err != nil {
		return fmt.Errorf("Error encountered retrieving worker nodes: %v", err)
	}

	var names []string

	if all {
		names = make([]string, len(nodes))
		for i, ni := range nodes {
			names[i] = ni.UniqName
		}
	} else if tag != "" {
		for _, ni := range nodes {
			if ni.HasTag(tag) {
				names = append(names, ni.UniqName)
			}
		}
	} else {
		validNames := make(map[string]struct{})
		for _, ni := range nodes {
			validNames[ni.UniqName] = struct{}{}
		}

		for _, n := range flags.Args() {
			if _, ok := validNames[n]; ok {
				names = append(names, n)
			}
		}
	}

	if len(names) == 0 {
		fmt.Println("no matching nodes were found")
		return nil
	}

	fmt.Printf("asking %d nodes to shutdown:\n", len(names))
	for i, n := range names {
		if i%3 == 0 {
			fmt.Printf("%s", n)
		} else if (i+1)%3 == 0 {
			fmt.Printf(" %s\n", n)
		} else {
			fmt.Printf(" %s", n)
		}
	}
	if len(names)%3 != 0 {
		fmt.Println()
	}

	msg := struct {
		UniqNames []string `json:"uniq_names"`
	}{
		UniqNames: names,
	}

	if err := callEndpoint(cfg, "client/nodes/shutdown", &msg, nil); err != nil {
		return fmt.Errorf("Error encountered shutting down worker nodes: %v", err)
	}

	return nil
}

type Usage struct {
	cmd, args string
	flags     *flag.FlagSet
}

func (u Usage) ShowAndExit() {
	u.Show()
	os.Exit(1)
}

func (u Usage) Show() {
	program := os.Args[0]
	if u.cmd == "" {
		/* global */
		fmt.Fprintf(os.Stderr,
			"usage: %s [global_flags] <cmd> [flags] <args>\nWhere <cmd> is one of\n",
			program)
		cmds := make([]string, 0, len(subCmds))
		for k, _ := range subCmds {
			cmds = append(cmds, k)
		}
		sort.Strings(cmds)
		for _, n := range cmds {
			c := subCmds[n]
			fmt.Fprintf(os.Stderr, "\t%s\t%s\n", n, c.Description)
		}
		fmt.Fprintf(os.Stderr, "\nGlobal flags:\n")
		globalSet.PrintDefaults()
	} else {
		fmt.Fprintf(os.Stderr, "usage: %s [global_flags] %s [flags] %s\n",
			program, u.cmd, u.args)
		fmt.Fprintf(os.Stderr, "\nGlobal flags:\n")
		globalSet.PrintDefaults()
		if u.flags != nil {
			fmt.Fprintf(os.Stderr, "\nFlags for %s\n", u.cmd)
			u.flags.PrintDefaults()
		}
	}
}

func main() {
	var err error
	subCmds = map[string]Command{
		"queue":   Command{"queues a new job", queueCmd},
		"status":  Command{"lists jobs", statusCmd},
		"cancel":  Command{"cancels jobs", changeStateCmd},
		"hold":    Command{"pauses queuing of a job", changeStateCmd},
		"pause":   Command{"pauses queuing of a job", changeStateCmd},
		"unhold":  Command{"resumes queuing of a job", changeStateCmd},
		"release": Command{"resumes queuing of a job", changeStateCmd},
		"wlist":   Command{"list workers", workerListCmd},
		"wstop":   Command{"stop workers", workerStopCmd},
	}

	usage := Usage{}
	if err = globalSet.Parse(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing global flags: %v", err)
		usage.ShowAndExit()
	}

	pv, err := fuq.SetupPaths()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error setting up paths: %v", err)
		os.Exit(1)
	}

	if globalSet.NArg() < 1 {
		usage.ShowAndExit()
	}

	cmdArgs := globalSet.Args()
	cmd, ok := subCmds[cmdArgs[0]]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown command %s\n", cmdArgs[0])
		usage.ShowAndExit()
	}

	if srvConfigFile == "" {
		if srvConfigFile = fuq.DefaultServerConfigPath(); srvConfigFile == "" {
			fmt.Fprintf(os.Stderr, "cannot determine default server config file\n")
			os.Exit(1)
		}
	}

	if sysConfigFile == "" {
		if sysConfigFile = fuq.DefaultSystemConfigPath(); sysConfigFile == "" {
			fmt.Fprintf(os.Stderr, "cannot determine default system config file\n")
			os.Exit(1)
		}
	}

	if err := config.ReadConfig(sysConfigFile, pv); err != nil {
		fmt.Fprintf(os.Stderr, "error reading config file '%s': %v\n",
			sysConfigFile, err)
		os.Exit(1)
	}

	if err := config.ReadConfig(srvConfigFile, pv); err != nil {
		fmt.Fprintf(os.Stderr, "error reading config file '%s': %v\n",
			srvConfigFile, err)
		os.Exit(1)
	}

	if err := cmd.Run(config, cmdArgs); err != nil {
		fmt.Fprintf(os.Stderr, "error encountered: %v\n", err)
		os.Exit(1)
	}
}

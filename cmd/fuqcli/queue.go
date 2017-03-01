package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/sfstewman/fuq"
	"os"
)

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

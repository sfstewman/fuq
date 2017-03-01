package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/sfstewman/fuq"
	"os"
	"sort"
)

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

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/sfstewman/fuq"
	"os"
)

func workerListCmd(cfg fuq.Config, args []string) error {
	var outputJSON = false
	var msg struct {
		CookieList bool `json:"cookie_list"`
		JobList    bool `json:"job_list"`
	}

	flags := flag.NewFlagSet("workerList", flag.ContinueOnError)
	flags.BoolVar(&outputJSON, "j", outputJSON, "output json response")
	flags.BoolVar(&msg.CookieList, "c", msg.CookieList, "display all nodes with valid cookies")
	flags.BoolVar(&msg.JobList, "l", msg.JobList, "display job data")

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
	if err := callEndpoint(cfg, "client/nodes/list", msg, &ret); err != nil {
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

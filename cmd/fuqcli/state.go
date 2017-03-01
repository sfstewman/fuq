package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/sfstewman/fuq"
	"os"
	"strconv"
)

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

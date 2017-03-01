package main

import (
	"flag"
	"fmt"
	"github.com/sfstewman/fuq"
	"github.com/sfstewman/fuq/srv"
	"os"
	"sort"
)

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

package main

import (
	"flag"
	"fmt"
	"github.com/sfstewman/fuq"
	"os"
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

	if err := config.ReadConfigFile(sysConfigFile, pv); err != nil {
		fmt.Fprintf(os.Stderr, "error reading config file '%s': %v\n",
			sysConfigFile, err)
		os.Exit(1)
	}

	if err := config.ReadConfigFile(srvConfigFile, pv); err != nil {
		fmt.Fprintf(os.Stderr, "error reading config file '%s': %v\n",
			srvConfigFile, err)
		os.Exit(1)
	}

	if err := cmd.Run(config, cmdArgs); err != nil {
		fmt.Fprintf(os.Stderr, "error encountered: %v\n", err)
		os.Exit(1)
	}
}

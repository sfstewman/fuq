package main

/* What's left to do?
 * 1. Generate configuration file
 * 2. Configure TLS
 * 3. Read node file / automatic discovery of the foreman
 * 4. Testing on cluster
 * 5. Write unit tests?
 */

import (
	"flag"
	"fmt"
	"github.com/sfstewman/fuq"
	"github.com/sfstewman/fuq/srv"
	"log"
	"os"
	"os/signal"
	"os/user"
	"runtime"
	"sync"
	"time"
)

const (
	MaxConfigRetries = 10
	HelloTries       = 10
)

func startWorkers(wcfg *srv.WorkerConfig, nproc int, config fuq.Config, wg *sync.WaitGroup) error {
	if nproc <= 0 {
		return nil
	}

	ep, err := fuq.NewEndpoint(config)
	if err != nil {
		return fmt.Errorf("error starting workers: %v", err)
	}

	if err := wcfg.NewCookieWithRetries(ep, HelloTries); err != nil {
		return fmt.Errorf("error obtaining cookies: %v", err)
	}

	for i := 0; i < nproc; i++ {
		log.Printf("Starting worker %d", i+1)
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()

			w := srv.Worker{
				Seq:      seq,
				Endpoint: ep,
				Name:     wcfg.NodeInfo.Node,
				Config:   wcfg,
			}
			defer w.Close()

			w.Loop()
		}(i + 1)
	}

	return nil
}

func main() {
	var (
		err                          error
		isForeman, onlyWriteConfig   bool
		srvConfigFile, sysConfigFile string
		retryServerConfig            bool
		workerTag                    string
		initialWait                  int
		config                       fuq.Config
		pv                           *fuq.PathVars
		overwriteConfig              bool
		numCPUs                      int
		u                            *user.User
	)

	// command line options
	isForeman = false
	onlyWriteConfig = false

	pv, err = fuq.SetupPaths()
	if err != nil {
		log.Fatalf("error setting up paths: %v", err)
	}

	u, err = user.Current()
	if err != nil {
		log.Fatalf("error retrieving current user: %v", err)
	}

	// default config file
	srvConfigFile = fuq.DefaultServerConfigPath()
	sysConfigFile = fuq.DefaultSystemConfigPath()

	// Setup command-line flags
	flag.BoolVar(&isForeman, "f", false, "invoke fuq as foreman")
	flag.BoolVar(&onlyWriteConfig, "w", false, "invoke fuq and foreman, write config, and exit")
	flag.IntVar(&numCPUs, "np", 1, "number of concurrent cores")
	flag.StringVar(&srvConfigFile, "srv", srvConfigFile, "server configuration file")
	flag.StringVar(&sysConfigFile, "cfg", sysConfigFile, "configuration file")
	flag.BoolVar(&overwriteConfig, "force_cfg", overwriteConfig,
		"overwrite config file (if foreman)")

	flag.IntVar(&initialWait, "wait0", initialWait, "seconds to wait before starting")
	flag.BoolVar(&retryServerConfig, "retry_cfg", retryServerConfig,
		"retries reading the server config on error")

	flag.StringVar(&config.DbPath, "db", "", "path to database")
	flag.StringVar(&config.LogDir, "log", "", "queue logging directory")
	flag.StringVar(&config.ForemanLogFile, "flog", "", "foreman logging file")
	flag.IntVar(&config.Port, "port", 0, "foreman port")

	flag.StringVar(&config.KeyFile, "key", "", "path to TLS key file")
	flag.StringVar(&config.CertFile, "cert", "", "path to TLS cert file")
	flag.StringVar(&config.RootCAFile, "ca", "", "path to TLS root ca file")
	flag.StringVar(&config.CertName, "certname", "", "name in TLS certificate")

	flag.StringVar(&workerTag, "tag", "", "tag for workers")

	flag.Parse()

	if srvConfigFile == "" {
		log.Fatalf("No default or given server config file")
	}

	if sysConfigFile == "" {
		log.Fatalf("No default or given config file")
	}

	if initialWait > 0 {
		log.Printf("sleeping %d seconds before startup", initialWait)
		time.Sleep(time.Duration(initialWait) * time.Second)
	}

	if onlyWriteConfig {
		isForeman = true
	}

	if isForeman {
		if err := config.ReadConfigFile(sysConfigFile, pv); err != nil {
			// igore if the system configuration file does not exist
			if !os.IsNotExist(err) {
				log.Fatalf("error reading config file '%s': %v",
					sysConfigFile, err)
			}
		}

		if err := config.GenerateConfigFile(srvConfigFile, pv, overwriteConfig); err != nil {
			log.Fatalf("error generating config file '%s': %v",
				srvConfigFile, err)
		}

		if onlyWriteConfig {
			os.Exit(0)
		}
	} else {
		config0 := config

		for retries := 0; true; retries++ {
			err := config.ReadConfigFile(srvConfigFile, pv)
			if err == nil {
				break
			}
			log.Printf("error reading config file '%s': %v",
				srvConfigFile, err)

			if !retryServerConfig || retries >= MaxConfigRetries {
				os.Exit(1)
			}

			time.Sleep(2 * time.Second) // XXX: remove hard-coded value

			// reset in case we had a bad read
			config = config0
		}

		if err := config.ReadConfigFile(sysConfigFile, pv); err != nil {
			// igore if the system configuration file does not exist
			if !os.IsNotExist(err) {
				log.Fatalf("error reading config file '%s': %v",
					sysConfigFile, err)
			}
		}
	}

	config.SetupTLS(fuq.DefaultConfigDir(u))

	fmt.Printf("Configuration is:\n%#v\n\n", config)

	// Start
	if numCPUs <= 0 || numCPUs > runtime.NumCPU() {
		numCPUs = runtime.NumCPU()
	}

	log.Printf("Limiting to %d CPUs", numCPUs)
	runtime.GOMAXPROCS(numCPUs)

	log.Printf("Log directory is %s", config.LogDir)
	if err := os.Mkdir(config.LogDir, 0700); err != nil && !os.IsExist(err) {
		log.Fatalf("error making logging directory: %v", err)
	}

	done := make(chan struct{})
	intr := make(chan os.Signal, 1)
	signal.Notify(intr, os.Interrupt)

	defer log.Println("Exiting...")

	nproc := numCPUs

	wg := sync.WaitGroup{}

	if isForeman {
		log.Printf("Starting foreman")

		go func() {
			f, err := srv.NewForeman(config, done)
			defer close(done)

			if err != nil {
				log.Printf("error starting foreman: %v", err)
				return
			}
			defer f.Close()

			if err := f.Run(); err != nil {
				log.Printf("error starting foreman: %v", err)
				return
			}
		}()

		// decrement so we only start N-1 workers
		nproc--
	}

	var tags []string
	if workerTag != "" {
		tags = []string{workerTag}
	}

	wc, err := srv.NewWorkerConfig(nproc, tags)
	if err != nil {
		log.Fatalf("error generating worker config: %v", err)
	}

	if err := startWorkers(wc, nproc, config, &wg); err != nil {
		log.Fatalf("error starting workers: %v", err)
	}

	if !isForeman {
		/* exit when the last worker is done.  this goroutine
		 * waits on wg and then issues a done signal
		 */
		go func() {
			defer close(done)
			wg.Wait()
			log.Println("Last worker is done, finishing up")
		}()
	}

runloop:
	for {
		select {
		case <-intr:
			// runtime.GC()
			log.Println("Keyboard interrupt detected... press again to shut down")
			select {
			case <-intr:
				log.Println("Keyboard interrupt detected... shutting down")
				break runloop
			case <-time.After(5 * time.Second):
				continue runloop
			}
		case <-done:
			log.Println("Programmatic quit signal... shutting down")
			break runloop
		}
	}
}

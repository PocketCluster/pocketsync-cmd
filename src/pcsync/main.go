/*
gosync is a command-line implementation of the gosync package functionality, primarily as a demonstration of usage
but supposed to be functional in itself.
*/
package main

import (
    "fmt"
    "log"
    "net/http"
    _ "net/http/pprof"
    "os"
    "runtime"

    "github.com/codegangsta/cli"
    "github.com/Redundancy/go-sync"
)

var app = cli.NewApp()

func main() {
    app.Name = "pcsync"
    app.Usage = "Build indexes, patches, patch files"
    app.Flags = []cli.Flag{
        cli.BoolFlag{
            Name:  "profile",
            Usage: "enable HTTP profiling",
        },
        cli.IntFlag{
            Name:  "profilePort",
            Value: 6060,
            Usage: "The number of streams to use concurrently",
        },
    }

    app.Version = fmt.Sprintf(
        "%v.%v.%v",
        gosync.PocketSyncMajorVersion,
        gosync.PocketSyncMinorVersion,
        gosync.PocketSyncPatchVersion,
    )

    runtime.GOMAXPROCS(runtime.NumCPU())

    app.Before = func(c *cli.Context) error {
        if c.Bool("profile") {
            port := fmt.Sprint(c.Int("profilePort"))

            go func() {
                log.Println(http.ListenAndServe("localhost:"+port, nil))
            }()
        }

        return nil
    }

    app.Run(os.Args)
}

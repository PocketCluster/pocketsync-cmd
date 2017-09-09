package main

import (
    "encoding/base64"
    "fmt"
    "io/ioutil"
    "os"
    "path/filepath"

    log "github.com/Sirupsen/logrus"
    "github.com/pkg/errors"
    "github.com/Redundancy/go-sync/filechecksum"
    "github.com/codegangsta/cli"
)

const (
    metaUsage string = "Meta JSON checksum generation'pcsync meta <meta file>'. *use in build script*"
)

func init() {
    app.Commands = append(
        app.Commands,
        cli.Command{
            Name:      "meta",
            ShortName: "m",
            Usage:     metaUsage,
            Action:    Meta,
        },
    )
}

func Meta(c *cli.Context) {
    log.SetLevel(log.DebugLevel)

    errorWrapper(c, func(c *cli.Context) error {
        if len(c.Args()) < 1 {
            return errors.Errorf("Usage is \"%v\" (invalid number of arguments)", metaUsage)
        }
        var (
            metaFileName  = c.Args()[0]
            hasher = filechecksum.DefaultStrongHashGenerator()
        )
        // get the exact path
        absFilePath, err := filepath.Abs(metaFileName)
        if err != nil {
            handleFileError(absFilePath, err)
            return err
        }
        // get the data
        metaData, err := ioutil.ReadFile(absFilePath)
        if err != nil {
            handleFileError(absFilePath, err)
            return err
        }

        hasher.Write(metaData)
        fmt.Fprint(os.Stdout, base64.URLEncoding.EncodeToString(hasher.Sum(nil)))
        return nil
    })
}
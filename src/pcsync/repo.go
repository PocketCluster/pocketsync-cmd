package main

import (
    "bufio"
    "encoding/json"
    "os"
    "path/filepath"

    log "github.com/Sirupsen/logrus"
    "github.com/pkg/errors"
    "github.com/urfave/cli"
)

const (
    repoUsage string = "Package repository list generation. 'pcsync repo <source list> <list output>'. *use in build script*"
)

func init() {
    app.Commands = append(
        app.Commands,
        cli.Command{
            Name:      "repo",
            ShortName: "rp",
            Usage:     repoUsage,
            Action:    Repolist,
        },
    )
}

func Repolist(c *cli.Context) {
    log.SetLevel(log.DebugLevel)

    errorWrapper(c, func(c *cli.Context) error {
        if len(c.Args()) < 2 {
            return errors.Errorf("Usage is \"%v\" (invalid number of arguments)", repoUsage)
        }

        var (
            srcList    string = c.Args()[0]
            listOut    string = c.Args()[1]
        )

        absSourcePath, err := filepath.Abs(srcList)
        if err != nil {
            handleFileError(absSourcePath, err)
            return err
        }
        refListReader, err := os.Open(absSourcePath)
        if err != nil {
            handleFileError(absSourcePath, err)
            return err
        }
        defer refListReader.Close()

        absOutputPath, err := filepath.Abs(listOut)
        if err != nil {
            handleFileError(absOutputPath, err)
            return err
        }
        outputFile, err := os.Create(absOutputPath)
        if err != nil {
            handleFileError(absOutputPath, err)
            return err
        }
        defer outputFile.Close()

        // read repository list
        var (
            scanner  *bufio.Scanner = bufio.NewScanner(refListReader)
            sourceList []string = nil
        )
        for scanner.Scan() {
            sourceList = append(sourceList, scanner.Text())
        }
        err = scanner.Err()
        if err != nil {
            return errors.WithStack(err)
        }

        err = json.NewEncoder(outputFile).Encode(sourceList)
        if err != nil {
            log.Errorf(errors.WithStack(err).Error())
            return err
        }

        return nil
    })
}


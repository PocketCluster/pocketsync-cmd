package main

import (
    "encoding/json"
    "io/ioutil"
    "os"
    "path/filepath"

    log "github.com/Sirupsen/logrus"
    "github.com/pkg/errors"
    "github.com/codegangsta/cli"
    "github.com/stkim1/pc-core/model"
)

const (
    listUsage string = "Package list generation. 'pcsync pkglist <core chksum> <node chksum> <meta chksum> <pkg ver> <list template input> <list output>'. *use in build script*"
)

func init() {
    app.Commands = append(
        app.Commands,
        cli.Command{
            Name:      "pkglist",
            ShortName: "pl",
            Usage:     listUsage,
            Action:    Pkglist,
        },
    )
}

func Pkglist(c *cli.Context) {
    log.SetLevel(log.DebugLevel)

    errorWrapper(c, func(c *cli.Context) error {
        if len(c.Args()) < 6 {
            return errors.Errorf("Usage is \"%v\" (invalid number of arguments)", listUsage)
        }

        var (
            coreChksum string = c.Args()[0]
            nodeChksum string = c.Args()[1]
            metaChksum string = c.Args()[2]
            pkgVer     string = c.Args()[3]
            templateIn string = c.Args()[4]
            listOut    string = c.Args()[5]
            pkgModel   *model.Package = &model.Package{}
        )

        absTemplPath, err := filepath.Abs(templateIn)
        if err != nil {
            handleFileError(absTemplPath, err)
            return err
        }
        tmplData, err := ioutil.ReadFile(absTemplPath)
        if err != nil {
            handleFileError(absTemplPath, err)
            return err
        }

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

        err = json.Unmarshal(tmplData, pkgModel)
        if err != nil {
            log.Errorf(errors.WithStack(err).Error())
            return err
        }

        pkgModel.PkgVer = pkgVer
        pkgModel.MetaChksum = metaChksum
        pkgModel.CoreImageChksum = coreChksum
        pkgModel.NodeImageChksum = nodeChksum

        err = json.NewEncoder(outputFile).Encode([]*model.Package{pkgModel})
        if err != nil {
            log.Errorf(errors.WithStack(err).Error())
            return err
        }

        return nil
    })
}

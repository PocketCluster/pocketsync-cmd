package main

import (
    "encoding/json"
    "io/ioutil"
    "os"
    "path/filepath"

    log "github.com/Sirupsen/logrus"
    "github.com/pkg/errors"
    "github.com/urfave/cli"
    "github.com/stkim1/pc-core/model"
)

const (
    listUsage string = "Package list generation. 'pcsync pkglist <core chksum> <core size> <node chksum> <node size> <meta chksum> <pkg ver> <list template input> <list output>'. *use in build script*"
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

func Pkglist(c *cli.Context) error {
    log.SetLevel(log.DebugLevel)

    if len(c.Args()) < 6 {
        return errors.Errorf("Usage is \"%v\" (invalid number of arguments)", listUsage)
    }

    var (
        coreChksum  = c.Args()[0]
        coreImgSize = c.Args()[1]
        nodeChksum  = c.Args()[2]
        nodeImgSize = c.Args()[3]
        metaChksum  = c.Args()[4]
        pkgChksum   = c.Args()[5]
        templateIn  = c.Args()[6]
        listOut     = c.Args()[7]
        pkgModel    = &model.Package{}
    )

    absTemplPath, err := filepath.Abs(templateIn)
    if err != nil {
        handleFileError(absTemplPath, err)
        return err
    }
    tmplData, err := ioutil.ReadFile(absTemplPath)
    if err != nil {
        handleFileError(absTemplPath, err)
        return errors.WithStack(err)
    }

    absOutputPath, err := filepath.Abs(listOut)
    if err != nil {
        handleFileError(absOutputPath, err)
        return errors.WithStack(err)
    }
    outputFile, err := os.Create(absOutputPath)
    if err != nil {
        handleFileError(absOutputPath, err)
        return errors.WithStack(err)
    }
    defer outputFile.Close()

    err = json.Unmarshal(tmplData, pkgModel)
    if err != nil {
        log.Errorf(errors.WithStack(err).Error())
        return errors.WithStack(err)
    }

    pkgModel.PkgChksum       = pkgChksum
    pkgModel.MetaChksum      = metaChksum
    pkgModel.CoreImageChksum = coreChksum
    pkgModel.CoreImageSize   = coreImgSize
    pkgModel.NodeImageChksum = nodeChksum
    pkgModel.NodeImageSize   = nodeImgSize

    err = json.NewEncoder(outputFile).Encode([]*model.Package{pkgModel})
    if err != nil {
        return errors.WithStack(err)
    }

    return nil
}

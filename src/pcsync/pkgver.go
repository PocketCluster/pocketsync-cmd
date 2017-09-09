package main

import (
    "encoding/base64"
    "fmt"
    "os"

    log "github.com/Sirupsen/logrus"
    "github.com/pkg/errors"
    "github.com/Redundancy/go-sync/merkle"
    "github.com/codegangsta/cli"
)

const (
    pkgverUsage string = "Package version generation. 'pcsync pkgver <core image checksum> <node image checksum> <meta json checksum>'. *use in build script*"
)

func init() {
    app.Commands = append(
        app.Commands,
        cli.Command{
            Name:      "pkgver",
            ShortName: "pv",
            Usage:     pkgverUsage,
            Action:    Pkgver,
        },
    )
}

func Pkgver(c *cli.Context) {
    log.SetLevel(log.DebugLevel)

    errorWrapper(c, func(c *cli.Context) error {
        if len(c.Args()) < 3 {
            return errors.Errorf("Usage is \"%v\" (invalid number of arguments)", pkgverUsage)
        }
        var (
            coreValue string = c.Args()[0]
            nodeValue string = c.Args()[1]
            metaValue string = c.Args()[2]
        )

        coreChksum, err := base64.URLEncoding.DecodeString(coreValue)
        if err != nil {
            log.Error(errors.WithStack(err).Error())
            return err
        }

        nodeChksum, err := base64.URLEncoding.DecodeString(nodeValue)
        if err != nil {
            log.Error(errors.WithStack(err).Error())
            return err
        }

        metaChksum, err := base64.URLEncoding.DecodeString(metaValue)
        if err != nil {
            log.Error(errors.WithStack(err).Error())
            return err
        }


        pkgChksum, err := merkle.SimpleHashFromHashes([][]byte{coreChksum, nodeChksum, metaChksum})
        if err != nil {
            log.Error(errors.WithStack(err).Error())
            return err
        }

        fmt.Fprint(os.Stdout, base64.URLEncoding.EncodeToString(pkgChksum))
        return nil
    })
}

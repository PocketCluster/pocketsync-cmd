package main

import (
    "bytes"
    "os"
    "path/filepath"
    "time"

    log "github.com/Sirupsen/logrus"
    "github.com/pkg/errors"
    gosync "github.com/Redundancy/go-sync"
    "github.com/Redundancy/go-sync/filechecksum"
    "github.com/codegangsta/cli"
)

func init() {
    app.Commands = append(
        app.Commands,
        cli.Command{
            Name:      "build",
            ShortName: "b",
            Usage:     "build a .pcsync file for a file",
            Action:    Build,
            Flags: []cli.Flag{
                cli.IntFlag{
                    Name:  "blocksize",
                    Value: gosync.PocketSyncDefaultBlockSize,
                    Usage: "The block size to use for the gosync file",
                },
            },
        },
    )
}

func Build(c *cli.Context) {
    log.SetLevel(log.DebugLevel)
    var (
        filename  = c.Args()[0]
        blocksize = uint32(c.Int("blocksize"))
        generator = filechecksum.NewFileChecksumGenerator(uint(blocksize))
    )

    inputFile, err := os.Open(filename)
    if err != nil {
        absInputPath, err2 := filepath.Abs(filename)
        if err2 == nil {
            handleFileError(absInputPath, err)
        } else {
            handleFileError(filename, err)
        }
        os.Exit(1)
    }
    defer inputFile.Close()

    stat, err := inputFile.Stat()
    if err != nil {
        log.Error(errors.WithStack(err).Error())
        os.Exit(1)
    }

    var (
        file_size   = stat.Size()
        ext         = filepath.Ext(filename)
        outfilePath = filename[:len(filename)-len(ext)] + ".pcsync"
        outBuf      = new(bytes.Buffer)
    )

    start := time.Now()
    rtcs, blockcount, err := generator.BuildSequentialAndRootChecksum(inputFile, outBuf)
    end := time.Now()
    if err != nil {
        log.Error(errors.WithMessage(err, "Error generating checksum from " + filename).Error())
        os.Exit(1)
    }

    outputFile, err := os.Create(outfilePath)
    if err != nil {
        handleFileError(outfilePath, err)
        os.Exit(1)
    }
    defer outputFile.Close()

    if err = writeHeaders(
        outputFile,
        file_size,
        blocksize,
        blockcount,
        rtcs,
    ); err != nil {
        log.Error(errors.WithMessage(err, "Error getting file info:" + filename).Error())
        os.Exit(2)
    }

    wrLen, err := outputFile.Write(outBuf.Bytes())
    if err != nil {
        log.Error(errors.WithMessage(err, "Error saving checksum :" + filename).Error())
        os.Exit(2)
    }
    if wrLen != outBuf.Len() {
        log.Error(errors.Errorf("Error saving checksum to file: checksum length %v vs written %v", outBuf.Len(), wrLen).Error())
        os.Exit(2)
    }

    inputFileInfo, err := os.Stat(filename)
    if err != nil {
        log.Error(errors.WithMessage(err, "Error getting file info:" + filename).Error())
        os.Exit(2)
    }

    log.Infof("Filename %s/ BlockSize %v/ BlockCount %v/ RootChecksum %v", filename, blocksize, blockcount, rtcs)

    log.Infof("Index for %v file generated in %v (%v bytes/S)\n",
        inputFileInfo.Size(),
        end.Sub(start),
        float64(inputFileInfo.Size())/end.Sub(start).Seconds())
}

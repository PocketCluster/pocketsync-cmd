package main

import (
    "bytes"
    "encoding/base64"
    "fmt"
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
                cli.BoolFlag{
                    Name:  "quite",
                    Usage: "Supress verbose log",
                },
                cli.StringFlag{
                    Name:  "output-dir",
                    Usage: "output directory specified",
                },
            },
        },
    )
}

func Build(c *cli.Context) {
    log.SetLevel(log.DebugLevel)
    errorWrapper(c, func(c *cli.Context) error {
        var (
            filename    = c.Args()[0]
            outfilePath = filename[:len(filename)-len(filepath.Ext(filename))] + ".pcsync"
            blocksize   = uint32(c.Int("blocksize"))
            quite       = c.Bool("quite")
            outputDir   = c.String("output-dir")

            generator   = filechecksum.NewFileChecksumGenerator(uint(blocksize))
            outBuf      = new(bytes.Buffer)
        )

        absInputPath, err := filepath.Abs(filename)
        if err != nil {
            if !quite {
                handleFileError(absInputPath, err)
            }
            return err
        }
        inputFile, err := os.Open(absInputPath)
        if err != nil {
            if !quite {
                handleFileError(absInputPath, err)
            }
            return err
        }
        defer inputFile.Close()

        // if output is not specified...
        if len(outputDir) != 0 {
            outfilePath = filepath.Join(filepath.Dir(outputDir), filepath.Base(outfilePath))
        }
        absOutputPath, err := filepath.Abs(outfilePath)
        if err != nil {
            if !quite {
                handleFileError(absOutputPath, err)
            }
            return err
        }
        outputFile, err := os.Create(absOutputPath)
        if err != nil {
            if !quite {
                handleFileError(absOutputPath, err)
            }
            return err
        }
        defer outputFile.Close()

        start := time.Now()
        rtcs, blockcount, err := generator.BuildSequentialAndRootChecksum(inputFile, outBuf)
        end := time.Now()
        if err != nil {
            if !quite {
                log.Error(errors.WithMessage(err, "Error generating checksum from " + filename).Error())
            }
            return err
        }

        stat, err := inputFile.Stat()
        if err != nil {
            if !quite {
                log.Error(errors.WithStack(err).Error())
            }
            return err
        }
        file_size := stat.Size()

        if err = writeHeaders(
            outputFile,
            file_size,
            blocksize,
            blockcount,
            rtcs,
        ); err != nil {
            if !quite {
                log.Error(errors.WithMessage(err, "Error getting file info:"+filename).Error())
            }
            return err
        }

        wrLen, err := outputFile.Write(outBuf.Bytes())
        if err != nil {
            if !quite {
                log.Error(errors.WithMessage(err, "Error saving checksum :" + filename).Error())
            }
            return err
        }
        if wrLen != outBuf.Len() {
            if !quite {
                log.Error(errors.Errorf("Error saving checksum to file: checksum length %v vs written %v", outBuf.Len(), wrLen).Error())
            }
            return err
        }

        if !quite {
            log.Infof("Filename %s | BlockSize %v | BlockCount %v | RootChecksum %v | Index for %v file generated in %v",
                filename,
                blocksize,
                blockcount,
                rtcs,
                file_size,
                end.Sub(start))
        } else {
            fmt.Fprint(os.Stdout, base64.URLEncoding.EncodeToString(rtcs))
        }
        return nil
    })
}

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
                    Usage: "The block size to use for the gosync file",
                },
            },
        },
    )
}

func Build(c *cli.Context) {
    log.SetLevel(log.DebugLevel)
    errorWrapper(c, func(c *cli.Context) error {
        var (
            filename  = c.Args()[0]
            blocksize = uint32(c.Int("blocksize"))
            quite     = bool(c.Bool("quite"))
            generator = filechecksum.NewFileChecksumGenerator(uint(blocksize))
        )

        absInputPath, err := filepath.Abs(filename)
        if err != nil {
            handleFileError(absInputPath, err)
            return err
        }
        inputFile, err := os.Open(absInputPath)
        if err != nil {
            if !quite {
                log.Error(errors.WithStack(err).Error())
            }
            return err
        }
        defer inputFile.Close()

        stat, err := inputFile.Stat()
        if err != nil {
            if !quite {
                log.Error(errors.WithStack(err).Error())
            }
            return err
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
            if !quite {
                log.Error(errors.WithMessage(err, "Error generating checksum from " + filename).Error())
            }
            return err
        }

        outputFile, err := os.Create(outfilePath)
        if err != nil {
            if !quite {
                handleFileError(outfilePath, err)
            }
            return err
        }
        defer outputFile.Close()

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

        inputFileInfo, err := os.Stat(filename)
        if err != nil {
            if !quite {
                log.Error(errors.WithMessage(err, "Error getting file info:" + filename).Error())
            }
            return err
        }

        if !quite {
            log.Infof("Filename %s | BlockSize %v | BlockCount %v | RootChecksum %v | Index for %v file generated in %v (%v bytes/S)",
                filename,
                blocksize,
                blockcount,
                rtcs,
                inputFileInfo.Size(),
                end.Sub(start),
                float64(inputFileInfo.Size())/end.Sub(start).Seconds())
        } else {
            fmt.Fprint(os.Stdout, base64.URLEncoding.EncodeToString(rtcs))
        }
        return nil
    })
}

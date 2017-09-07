package main

import (
    "bufio"
    "os"
    "time"

    log "github.com/Sirupsen/logrus"
    "github.com/pkg/errors"
    "github.com/Redundancy/go-sync/blockrepository"
    "github.com/Redundancy/go-sync/blocksources"
    "github.com/Redundancy/go-sync/filechecksum"
    "github.com/Redundancy/go-sync/patcher"
    "github.com/Redundancy/go-sync/patcher/multisources"
    "github.com/codegangsta/cli"
)

const usage = "gosync patch <reference index> <reference repository list> <output>"

func init() {
    app.Commands = append(
        app.Commands,
        cli.Command{
            Name:      "patch",
            ShortName: "p",
            Usage:     usage,
            Description: `Recreate the reference source file, using an index and a local file that is believed to be similar.
The index should be produced by "gosync build".

<reference index> is a .gosync file and may be a local, unc network path or http/https url.
<reference repository list> is corresponding repository list in .text file format.
<output> is the local file will be overwritten when done.`,
            Action: Patch,
        },
    )
}

// Patch a file
func Patch(c *cli.Context) {
    errorWrapper(c, func(c *cli.Context) error {

        log.Println("Starting patching process")
        if len(c.Args()) < 3 {
            return errors.Errorf("Usage is \"%v\" (invalid number of arguments)", usage)
        }
        var (
            refIndexName  = c.Args()[0]
            refListName   = c.Args()[1]
            outFileName   = c.Args()[2]
        )
        if len(refIndexName) == 0 {
            return errors.Errorf("Usage is \"%v\" (invalid reference index filename)", usage)
        }
        if len(refListName) == 0 {
            return errors.Errorf("Usage is \"%v\" (invalid reference repository list filename)", usage)
        }
        if len(outFileName) == 0 {
            return errors.Errorf("Usage is \"%v\" (invalid output filename)", usage)
        }

        // index file
        indexReader, err := os.Open(refIndexName)
        if err != nil {
            return errors.WithStack(err)
        }
        // read repository list
        refListReader, err := os.Open(refListName)
        if err != nil {
            return errors.WithStack(err)
        }
        // otuput file
        outFile, err := os.Create(outFileName)
        if err != nil {
            return errors.WithStack(err)
        }
        defer func() {
            indexReader.Close()
            refListReader.Close()
            outFile.Close()
        }()

        // read index & build checksum
        filesize, blocksize, blockcount, rootHash, err := readHeadersAndCheck(indexReader)
        if err != nil {
            return errors.WithStack(err)
        }
        index, _, err := readIndex(indexReader, uint(blocksize), uint(blockcount), rootHash)
        if err != nil {
            return errors.WithStack(err)
        }

        // read repository list
        var (
            scanner  *bufio.Scanner = bufio.NewScanner(refListReader)
            resolver = blockrepository.MakeKnownFileSizedBlockResolver(int64(blocksize), filesize)
            verifier = &filechecksum.HashVerifier{
                Hash:                filechecksum.DefaultFileHashGenerator(),
                BlockSize:           uint(blocksize),
                BlockChecksumGetter: index,
            }

            sourceList []string = nil
            repoList   []patcher.BlockRepository = nil
        )
        for scanner.Scan() {
            sourceList = append(sourceList, scanner.Text())
        }
        err = scanner.Err()
        if err != nil {
            return errors.WithStack(err)
        }
        for rID, src := range sourceList {
            log.Infof("%v : %v", rID, src)
            repoList = append(repoList,
                blockrepository.NewBlockRepositoryBase(
                    uint(rID),
                    blocksources.NewRequesterWithTimeout(src, time.Duration(10) * time.Second),
                    resolver,
                    verifier))
        }
        msync, err := multisources.NewMultiSourcePatcher(outFile, repoList, index)
        if err != nil {
            return errors.WithStack(err)
        }

        log.Infof("Start patching %v for the size of %v", outFileName, filesize)
        err = msync.Patch()
        if err != nil {
            return errors.WithStack(err)
        }

        return errors.WithStack(msync.Close())
    })
}

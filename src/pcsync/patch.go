package main

import (
    "bufio"
    "fmt"
    "io"
    "os"
    "time"

    log "github.com/Sirupsen/logrus"
    "github.com/pkg/errors"
    "github.com/urfave/cli"
    "github.com/Redundancy/go-sync/blockrepository"
    "github.com/Redundancy/go-sync/blocksources"
    "github.com/Redundancy/go-sync/filechecksum"
    "github.com/Redundancy/go-sync/patcher"
    "github.com/Redundancy/go-sync/patcher/multisources"
    "github.com/Redundancy/go-sync/showpipe"
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
func Patch(c *cli.Context) error {
    log.SetLevel(log.DebugLevel)
    log.Infof("Starting patching process")
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
            Hash:                filechecksum.DefaultStrongHashGenerator(),
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
                blocksources.NewRequesterWithTimeout(src, "PocketCluster/0.1.4 (OSX)", false, time.Duration(10) * time.Second),
                resolver,
                verifier))
    }

    pipeReader, pipeWriter, pipeReporter := showpipe.PipeWithReport(uint64(filesize))
    defer func() {
        pipeReader.Close()
        pipeWriter.Close()
    }()
    go func() {
        for rpt := range pipeReporter {
            fmt.Fprint(os.Stdout, fmt.Sprintf("Recieved %v | Progress %.1f | Speed %.1f\r", rpt.Received, (rpt.DonePercent * 100.0), rpt.Speed / float64(1024 * 1024)))
        }
    }()
    msync, err := multisources.NewMultiSourcePatcher(pipeWriter, repoList, index)
    if err != nil {
        return errors.WithStack(err)
    }
    log.Infof("BlockSize %v/ BlockCount %v/ RootChecksum %v\nStart patching %v for the size of %v",blocksize, blockcount, rootHash, outFileName, filesize)
    go func() {
        _, err := io.Copy(outFile, pipeReader)
        if err != nil {
            log.Infof("%v", err.Error())
        }
    }()
    start := time.Now()
    err = msync.Patch()
    end := time.Now()
    if err != nil {
        return errors.WithStack(err)
    }
    log.Infof("Time duration %v | Data Rate %v/sec",end.Sub(start).Seconds(), int64(float64(filesize) / end.Sub(start).Seconds()))

    return errors.WithStack(msync.Close())
}

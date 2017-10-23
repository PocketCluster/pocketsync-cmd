package main

import (
    "runtime"
    "time"

    log "github.com/Sirupsen/logrus"
    "github.com/pkg/errors"
    "github.com/urfave/cli"
)

func init() {
    app.Commands = append(
        app.Commands,
        cli.Command{
            Name:        "diff",
            ShortName:   "d",
            Usage:       "gosync diff <localfile> <reference.gosync>",
            Description: `Compare a file with a reference index, and print statistics on the comparison and performance.`,
            Action:      Diff,
            Flags: []cli.Flag{
                cli.IntFlag{
                    Name:  "p",
                    Value: runtime.NumCPU(),
                    Usage: "The number of streams to use concurrently",
                },
            },
        },
    )
}

func Diff(c *cli.Context) error {
    var (
        localFilename     string = c.Args()[0]
        referenceFilename string = c.Args()[1]
        startTime      time.Time = time.Now()
    )
    log.SetLevel(log.DebugLevel)

    localFile := openFileAndHandleError(localFilename)
    if localFile == nil {
        return errors.Errorf("unable to open local file %v", localFilename)
    }
    defer localFile.Close()

    referenceFile := openFileAndHandleError(referenceFilename)
    if referenceFile == nil {
        return errors.Errorf("unable to open reference file %v", referenceFilename)
    }
    defer referenceFile.Close()

    _, blocksize, blockcount, rootHash, err := readHeadersAndCheck(referenceFile)
    if err != nil {
        return errors.WithMessage(err, "Error loading index")
    }

    log.Infof("Blocksize: %v", blocksize)
    index, _, err := readIndex(referenceFile, uint(blocksize), uint(blockcount), rootHash)
    referenceFile.Close()
    if err != nil {
        return errors.WithStack(err)
    }

    log.Infof("Weak hash count: %v", index.WeakCount())
    fi, err := localFile.Stat()
    if err != nil {
        return errors.WithMessage(err, "Could not get info on file:")
    }

    var (
        num_matchers   = int64(c.Int("p"))
        localFile_size = fi.Size()
    )
    // Don't split up small files
    if localFile_size < 1024*1024 {
        num_matchers = 1
    }

    merger, compare := multithreadedMatching(
        localFile,
        index,
        localFile_size,
        num_matchers,
        uint(blocksize),
    )

    mergedBlocks := merger.GetMergedBlocks()

    log.Infof("\nMatched:")
    totalMatchingSize := uint64(0)
    matchedBlockCountAfterMerging := uint(0)

    for _, b := range mergedBlocks {
        totalMatchingSize += uint64(b.EndBlock-b.StartBlock+1) * uint64(blocksize)
        matchedBlockCountAfterMerging += b.EndBlock - b.StartBlock + 1
    }

    log.Infof("Comparisons: %v", compare.Comparisons)
    log.Infof("Weak hash hits: %v", compare.WeakHashHits)

    if compare.Comparisons > 0 {
        log.Infof(
            "Weak hit rate: %.2f%%\n",
            100.0*float64(compare.WeakHashHits)/float64(compare.Comparisons),
        )
    }

    log.Infof("Strong hash hits:", compare.StrongHashHits)
    if compare.WeakHashHits > 0 {
        log.Infof(
            "Weak hash error rate: %.2f%%\n",
            100.0*float64(compare.WeakHashHits-compare.StrongHashHits)/float64(compare.WeakHashHits),
        )
    }

    log.Infof("Total matched bytes: %v", totalMatchingSize)
    log.Infof("Total matched blocks: %v", matchedBlockCountAfterMerging)

    // TODO: GetMissingBlocks uses the highest index, not the count, this can be pretty confusing
    // Should clean up this interface to avoid that
    missing := mergedBlocks.GetMissingBlocks(uint(index.BlockCount) - 1)
    log.Infof("Index blocks: %v", index.BlockCount)

    totalMissingSize := uint64(0)
    for _, b := range missing {
        //fmt.Printf("%#v\n", b)
        totalMissingSize += uint64(b.EndBlock-b.StartBlock+1) * uint64(blocksize)
    }

    log.Infof("Approximate missing bytes: %v", totalMissingSize)
    log.Infof("Time taken: %v", time.Now().Sub(startTime))
    return nil
}

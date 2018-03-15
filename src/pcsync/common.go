package main

import (
    "bufio"
    "bytes"
    "encoding/binary"
    "io"
    "net/http"
    "net/url"
    "os"

    log "github.com/Sirupsen/logrus"
    "github.com/pkg/errors"
    "github.com/urfave/cli"
    gosync "github.com/Redundancy/go-sync"
    "github.com/Redundancy/go-sync/chunks"
    "github.com/Redundancy/go-sync/comparer"
    "github.com/Redundancy/go-sync/filechecksum"
    "github.com/Redundancy/go-sync/index"
    "github.com/Redundancy/go-sync/patcher"
)

const (
    // KB - One Kilobyte
    KB = 1024
    // MB - One Megabyte
    MB = 1000000
)

func errorWrapper(c *cli.Context, f func(*cli.Context) error) {
    defer func() {
        if p := recover(); p != nil {
            log.Errorf("%v", p)
            os.Exit(1)
        }
    }()

    if err := f(c); err != nil {
        log.Errorf(err.Error())
        os.Exit(1)
    }

    return
}

func openFileAndHandleError(filename string) (f *os.File) {
    var err error
    f, err = os.Open(filename)

    if err != nil {
        f = nil
        handleFileError(filename, err)
    }

    return
}

func formatFileError(filename string, err error) error {
    switch {
    case os.IsExist(err):
        return errors.Errorf(
            "Could not open %v (already exists): %v",
            filename, err)
    case os.IsNotExist(err):
        return errors.Errorf(
            "Could not find %v: %v\n",
            filename, err)
    case os.IsPermission(err):
        return errors.Errorf(
            "Could not open %v (permission denied): %v\n",
            filename, err)
    default:
        return errors.Errorf(
            "Unknown error opening %v: %v\n",
            filename, err)
    }
}

func handleFileError(filename string, err error) {
    log.Error(errors.WithStack(formatFileError(filename, err)).Error())
}

func getLocalOrRemoteFile(path string) (io.ReadCloser, error) {
    url, err := url.Parse(path)

    switch {
    case err != nil:
        return os.Open(path)
    case url.Scheme == "":
        return os.Open(path)
    default:
        response, err := http.Get(path)

        if err != nil {
            return nil, err
        }

        if response.StatusCode < 200 || response.StatusCode > 299 {
            return nil, errors.Errorf("Request to %v returned status: %v", path, response.Status)
        }

        return response.Body, nil
    }
}

func toPatcherFoundSpan(sl comparer.BlockSpanList, blockSize int64) []patcher.FoundBlockSpan {
    result := make([]patcher.FoundBlockSpan, len(sl))

    for i, v := range sl {
        result[i].StartBlock = v.StartBlock
        result[i].EndBlock = v.EndBlock
        result[i].MatchOffset = v.ComparisonStartOffset
        result[i].BlockSize = blockSize
    }

    return result
}

func toPatcherMissingSpan(sl comparer.BlockSpanList, blockSize int64) []patcher.MissingBlockSpan {
    result := make([]patcher.MissingBlockSpan, len(sl))

    for i, v := range sl {
        result[i].StartBlock = v.StartBlock
        result[i].EndBlock = v.EndBlock
        result[i].BlockSize = blockSize
    }

    return result
}

func writeHeaders(
    f          *os.File,
    filesize   int64,
    blocksize  uint32,
    blockcount uint32,
    rootHash   []byte,
) error {
    if _, err := f.WriteString(gosync.PocketSyncMagicString); err != nil {
        return errors.WithStack(err)
    }
    for _, v := range []uint16{gosync.PocketSyncMajorVersion, gosync.PocketSyncMinorVersion, gosync.PocketSyncPatchVersion} {
        if err := binary.Write(f, binary.LittleEndian, v); err != nil {
            return errors.WithStack(err)
        }
    }
    if err := binary.Write(f, binary.LittleEndian, filesize); err != nil {
        return errors.WithStack(err)
    }
    if err := binary.Write(f, binary.LittleEndian, blocksize); err != nil {
        return errors.WithStack(err)
    }
    if err := binary.Write(f, binary.LittleEndian, blockcount); err != nil {
        return errors.WithStack(err)
    }
    var hLen uint32 = uint32(len(rootHash))
    if err := binary.Write(f, binary.LittleEndian, hLen); err != nil {
        return errors.WithStack(err)
    }
    if err := binary.Write(f, binary.LittleEndian, rootHash); err != nil {
        return errors.WithStack(err)
    }
    return nil
}

// reads the file headers and checks the magic string, then the semantic versioning
// return : in order of 'filesize', 'blocksize', 'blockcount', 'rootHash', 'error'
func readHeadersAndCheck(r io.Reader) (int64, uint32, uint32, []byte, error) {
    var (
        bMagic                []byte = make([]byte, len(gosync.PocketSyncMagicString))
        major, minor, patch   uint16 = 0, 0, 0
        filesize              int64  = 0
        blocksize, blockcount uint32 = 0, 0
        hLen                  uint32 = 0
        rootHash              []byte = nil
    )
    // magic string
    if _, err := r.Read(bMagic); err != nil {
        return 0, 0, 0, nil, errors.WithStack(err)
    } else if string(bMagic) != gosync.PocketSyncMagicString {
        return 0, 0, 0, nil, errors.New("meta header does not confirm. Not a valid meta")
    }

    // version
    for _, v := range []*uint16{&major, &minor, &patch} {
        if err := binary.Read(r, binary.LittleEndian, v); err != nil {
            return 0, 0, 0, nil, errors.WithStack(err)
        }
    }
    if major != gosync.PocketSyncMajorVersion || minor != gosync.PocketSyncMinorVersion || patch != gosync.PocketSyncPatchVersion {
        return 0, 0, 0, nil, errors.Errorf("The acquired version (%v.%v.%v) does not match the tool (%v.%v.%v).",
            major, minor, patch,
            gosync.PocketSyncMajorVersion, gosync.PocketSyncMinorVersion, gosync.PocketSyncPatchVersion)
    }

    if err := binary.Read(r, binary.LittleEndian, &filesize); err != nil {
        return 0, 0, 0, nil, errors.WithStack(err)
    }
    if err := binary.Read(r, binary.LittleEndian, &blocksize); err != nil {
        return 0, 0, 0, nil, errors.WithStack(err)
    }
    if err := binary.Read(r, binary.LittleEndian, &blockcount); err != nil {
        return 0, 0, 0, nil, errors.WithStack(err)
    }
    if err := binary.Read(r, binary.LittleEndian, &hLen); err != nil {
        return 0, 0, 0, nil, errors.WithStack(err)
    }
    rootHash = make([]byte, hLen)
    if _, err := r.Read(rootHash); err != nil {
        return 0, 0, 0, nil, errors.WithStack(err)
    }
    return filesize, blocksize, blockcount, rootHash, nil
}

func readIndex(rd io.Reader, blocksize, blockcount uint, rootHash []byte) (*index.ChecksumIndex, filechecksum.ChecksumLookup, error) {
    var (
        generator    = filechecksum.NewFileChecksumGenerator(blocksize)
        idx          *index.ChecksumIndex = nil
        chksumLookup filechecksum.ChecksumLookup  = nil
    )

    readChunks, err := chunks.CountedLoadChecksumsFromReader(
        rd,
        blockcount,
        generator.GetWeakRollingHash().Size(),
        generator.GetStrongHash().Size(),
    )
    if err != nil {
        return nil, nil, errors.WithStack(err)
    }

    chksumLookup = chunks.StrongChecksumGetter(readChunks)
    idx = index.MakeChecksumIndex(readChunks)
    cRootHash, err := idx.SequentialChecksumList().RootHash()
    if err != nil {
        return nil, nil, errors.WithStack(err)
    }
    if bytes.Compare(cRootHash, rootHash) != 0 {
        return nil, nil, errors.Errorf("[ERR] mismatching integrity checksum")
    }

    return idx, chksumLookup, nil
}

func multithreadedMatching(
    localFile     *os.File,
    idx           *index.ChecksumIndex,
    localFileSize int64,
    matcherCount  int64,
    blocksize     uint,
) (*comparer.MatchMerger, *comparer.Comparer) {
    // Note: Since not all sections of the file are equal in work
    // it would be better to divide things up into more sections and
    // pull work from a queue channel as each finish
    sectionSize := localFileSize / matcherCount
    sectionSize += int64(blocksize) - (sectionSize % int64(blocksize))
    merger := &comparer.MatchMerger{}
    compare := &comparer.Comparer{}

    for i := int64(0); i < matcherCount; i++ {
        offset := sectionSize * i

        // Sections must overlap by blocksize (strictly blocksize - 1?)
        if i > 0 {
            offset -= int64(blocksize)
        }

        sectionReader := bufio.NewReaderSize(
            io.NewSectionReader(localFile, offset, sectionSize),
            MB,
        )

        sectionGenerator := filechecksum.NewFileChecksumGenerator(uint(blocksize))

        matchStream := compare.StartFindMatchingBlocks(
            sectionReader, offset, sectionGenerator, idx)

        merger.StartMergeResultStream(matchStream, int64(blocksize))
    }

    return merger, compare
}

// better way to do this?

package main

import (
	"crypto/md5"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	// "fmt"
	// "strconv"
	// "github.com/DataDog/zstd"
)

// Used to store job progress
type Job struct {
	dir      string
	currFile string
	files    []string

	/*
	*	https://golang.org/doc/effective_go#maps
	*	An attempt to fetch a map value with a key that is not present
	*	in the map will return the zero value for the type of the entries in the map
	 */
	hashes map[string]bool // All files in dir (permanent)
}

func main() {

	// Pass filedir to NewJob()
	job, err := NewJob("testfiles")
	if err != nil {
		log.Fatal(err)
	}

	err = job.Run()
	if err != nil {
		log.Fatal(err)
	}
}

// NewJob sets a starting point for Divider to use
func NewJob(dir string) (Job, error) {

	// Check to see if dir exists, and is a dir
	info, err := os.Stat(dir)
	if err != nil {
		return Job{}, err
	} else if !info.IsDir() {
		return Job{}, errors.New("is not a directory")
	}

	// Get dir pointer
	f, err := os.Open(dir)
	if err != nil {
		return Job{}, err
	}
	defer f.Close()

	// Check if dir is empty before creating entry point
	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return Job{}, errors.New("directory is empty")
	}

	return Job{
		dir:    dir,
		hashes: make(map[string]bool),
	}, nil
}

func (j *Job) Run() error {

	// Open dir entry point
	files, err := ioutil.ReadDir(j.dir)
	if err != nil {
		return nil
	}

	for _, fileInfo := range files {
		opts, err := NewOptions(kiB/4, kiB*4, kiB)
		if err != nil {
			return err
		}

		f, err := os.Open(fileInfo.Name())
		if err != nil {
			return err
		}
		defer f.Close()

		divider, err := NewDivider(f, opts)
		if err != nil {
			return err
		}

		for {
			// Start looping through chunks
			chunk, err := divider.Next()
			if err != nil {
				if err == io.EOF {
					break
				}

				return err
			}

			if !j.hashes[md5.Sum(chunk.data)] {

			}
			// Chunk doesn't exist
			// if !j.chunks[string(chunk.fingerprint)] {
			// 	err = saveChunk("chunks/", chunk)
			// 	if err != nil {
			// 		return err
			// 	}

			// 	j.chunks[string(chunk.fingerprint)] = true
			// }
		}

		_ = chunks
	}

	// Loop through nested dirs
	// Apply FastCDC to file
	// Store new chunks on memory
	// Save unique file hashes
	// Reset per file

	return errors.New("error")
}

// func (j *Job) generateFileHashes error {

// 	// Get dir pointer
// 	f, err := os.Open(dir)
// 	if err != nil {
// 		return Job{}, err
// 	}
// 	defer f.Close()

// 	// Check if dir is empty before creating starting point
// 	_, err = f.Readdirnames(1)
// 	if err == io.EOF {
// 		return Job{}, errors.New("directory is empty")
// 	}

// 	// Get all files
// 	files, err := f.Readdirnames(0)
// 	if err != nil {
// 		return Job{}, err
// 	}

// 	// type Chunk struct {
// 	// 	offset      int
// 	// 	length      int
// 	// 	data        []byte
// 	// 	fingerprint uint64
// 	// }

// 	// chunks := getFileChunks();
// 	// for _, chunk in range chunks {
// 	// 	j.hashes[md5.Sum(chunk.Data)] =
// 	// }

// 	// md5.Sum(chunk.Data)
// 	// Get file chunks
// 	// Add md5 to hashes
// 	// Continue

// 	return errors.New("error");
// }

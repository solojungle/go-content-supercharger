package main

import (
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/DataDog/zstd"
	// "strconv"
)

// Used to store job progress
type Job struct {
	dir    string
	chunks []Chunk

	/*
		https://golang.org/doc/effective_go#maps
		An attempt to fetch a map value with a key that is not present
		in the map will return the zero value for the type of the entries in the map
	**/
	hashes map[string]bool
}

func main() {

	// Pass filedir to NewJob()
	job, err := NewJob("testfiles")
	if err != nil {
		log.Fatal(err)
	}

	// Set all chunks in Job state
	err = job.Run()
	if err != nil {
		log.Fatal(err)
	}

	// Save chunks to specific dir
	err = job.Save("./out")
	if err != nil {
		log.Fatal(err)
	}
}

/*
	NewJob() sets a starting point for Divider to use
**/
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

/*
	Run() loops over a directory and determines redundant data in files will
	set Job state to valid chunks
**/
func (j *Job) Run() error {

	// Open dir entry point
	files, err := ioutil.ReadDir(j.dir)
	if err != nil {
		return nil
	}

	// Loop over files in dir
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

		// Get chunks from file
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

			// Convert [16]byte to string
			b := md5.Sum(chunk.data)
			hashKey := string(b[:])
			chunk.md5 = hashKey

			// Check if chunk does not exist
			if !j.hashes[hashKey] {

				// Set toggle
				j.hashes[hashKey] = true

				// Add chunk to mem
				j.chunks = append(j.chunks, chunk)
			}
		}
	}

	return nil
}

/*
	Save() loops over chunks in the job, saves them
	then resets the buffer
**/
func (j *Job) Save(dir string) error {

	// Create dir if it doesn't exist
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.Mkdir(dir, 0700)
	}

	// Save each chunk
	for _, chunk := range j.chunks {

		filePath := dir + "/" + chunk.md5

		_ = filePath

		// Check if chunk already exists
		if _, err := os.Stat(filePath); err != nil {
			if os.IsNotExist(err) {
				fmt.Println("chunk already exists: ", chunk.md5)
				continue
			}
		}

		// Compress/overwrite chunk data
		_, err := zstd.CompressLevel(chunk.data, chunk.data, 19)
		if err != nil {
			return err
		}

		err = ioutil.WriteFile(filePath, chunk.data, 0644)
		if err != nil {
			return err
		}

		fmt.Println("Saving chunk hash: ", chunk.md5)
	}

	return nil
}

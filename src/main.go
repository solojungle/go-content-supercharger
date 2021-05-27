package main

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

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

	// Create options for divider
	opts, err := NewOptions(kiB/4, kiB*4, kiB)
	if err != nil {
		return err
	}

	// Create list of files
	files := make([]os.FileInfo, 0)

	// Use walk to get all sub-directories
	err = filepath.Walk(j.dir, func(path string, f os.FileInfo, err error) error {

		// Append only files to filelist
		if !f.IsDir() {
			files = append(files, f)
		}

		return err
	})

	fmt.Println(files)

	if err != nil {
		return err
	}

	// Loop over files in dir
	for _, fileInfo := range files {

		f, err := os.Open(j.dir + "/" + fileInfo.Name())
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

			// Generate md5 hash from data
			hashKey := chunk.hashChunkMd5()

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

		// Check if chunk already exists
		_, err := os.Stat(filePath)
		if err == nil {
			fmt.Println("Chunk already exists not saving: ", chunk.md5)
			continue
		}

		// File does not exist create new chunk
		// Compress/overwrite chunk data
		_, err = zstd.CompressLevel(chunk.data, chunk.data, 19)
		if err != nil {
			return err
		}

		// Save chunk
		err = ioutil.WriteFile(filePath, chunk.data, 0644)
		if err != nil {
			return err
		}

		fmt.Println("Saving chunk hash: ", chunk.md5)
	}

	return nil
}

func (c *Chunk) hashChunkMd5() string {
	hash := md5.Sum(c.data)
	c.md5 = hex.EncodeToString(hash[:])
	return c.md5
}

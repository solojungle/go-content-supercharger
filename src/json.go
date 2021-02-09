/**
*	json.go
*	Store a set of predefined structs to marshal/unmarshal JSON files
**/

package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// an array of users
type Progress struct {
	HasFinished bool             `json:"hasFinished"`
	Directory   string           `json:"directory"`
	CurrentFile string           `json:"currentFile"`
	Chunks      ChunkInformation `json:"chunks"`
}

type ChunkInformation struct {
	total     int `json:"total"`
	remaining int `json:"remaining"`
	duplicate int `json:"duplicate"`
}

func writeJSON(file string, p Progress) error {

	f, err := os.OpenFile(file, os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	defer f.Close()

	progressJSON, err := json.Marshal(p)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(file, progressJSON, 0644)
	if err != nil {
		return err
	}

	return nil
}

func readJSON(file string) (Progress, error) {
	// File doesn't exist
	if _, err := os.Stat(file); os.IsNotExist(err) {
		return Progress{}, err
	}

	f, err := os.Open(file)
	if err != nil {
		return Progress{}, err
	}

	defer f.Close()

	buffer, _ := ioutil.ReadAll(f)

	var progress Progress

	json.Unmarshal(buffer, &progress)

	return progress, nil
}

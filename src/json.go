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

func writeJSON(p Progress) error {

	f, err := os.OpenFile("progress.json", os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	defer f.Close()

	progressJSON, err := json.Marshal(p)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile("progress.json", progressJSON, 0644)
	if err != nil {
		return err
	}

	return nil
}

func readJSON() (Progress, error) {
	// File doesn't exist
	if _, err := os.Stat("progress.json"); os.IsNotExist(err) {
		return Progress{}, err
	}

	f, err := os.Open("progress.json")
	if err != nil {
		return Progress{}, err
	}

	defer f.Close()

	buffer, _ := ioutil.ReadAll(f)

	var progress Progress

	json.Unmarshal(buffer, &progress)

	return progress, nil
}

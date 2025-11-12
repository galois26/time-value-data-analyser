// internal/store/state.go
package store

import (
	"encoding/json"
	"log"
	"os"
)

type GTAState struct {
	LastAnnounced string `json:"last_announced"`
}

type NewsState struct {
	LastPublished string `json:"last_published"`
}

func LoadGTAState(path string) (GTAState, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return GTAState{}, err
	}
	var s GTAState
	return s, json.Unmarshal(b, &s)
}

func SaveGTAState(path string, s GTAState) error {
	b, err := json.MarshalIndent(s, "", " ")
	if err != nil {
		log.Printf("marshal gta state: %v", err)
	}
	return os.WriteFile(path, b, 0644)
}

func LoadNewsState(path string) (NewsState, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return NewsState{}, err
	}
	var s NewsState
	return s, json.Unmarshal(b, &s)
}

func SaveNewsState(path string, s NewsState) error {
	b, err := json.MarshalIndent(s, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0644)
}

package motifsdb

import (
	"sync"

	"github.com/antonybholmes/go-motifs"
)

type CacheMode string

var (
	instance *motifs.MotifDB
	once     sync.Once
)

func InitMotifDB(file string) *motifs.MotifDB {
	once.Do(func() {
		instance = motifs.NewMotifDB(file)
	})

	return instance
}

func Instance() *motifs.MotifDB {
	return instance
}

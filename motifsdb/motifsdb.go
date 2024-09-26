package motifsdb

import (
	"sync"

	"github.com/antonybholmes/go-motifs"
)

var instance *motifs.MotifDB
var once sync.Once

func InitCache(file string) *motifs.MotifDB {
	once.Do(func() {
		instance = motifs.NewMotifDB(file)
	})

	return instance
}

func GetInstance() *motifs.MotifDB {
	return instance
}

func Convert(search string) (*motifs.Motif, error) {
	return instance.Convert(search)
}

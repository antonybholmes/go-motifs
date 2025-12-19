package motifsdb

import (
	"sync"

	"github.com/antonybholmes/go-motifs"
)

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

func GetInstance() *motifs.MotifDB {
	return instance
}

func Datasets() ([]*motifs.Dataset, error) {
	return instance.Datasets()
}

func Search(search string, page int, pageSize int, reverse bool, complement bool) (*motifs.MotifSearchResult, error) {
	return instance.Search(search, page, pageSize, reverse, complement)
}

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

func GetInstance() *motifs.MotifDB {
	return instance
}

func Datasets(useCache bool) ([]*motifs.Dataset, error) {
	return instance.Datasets(useCache)
}

func Search(queries []string,
	datasets []string,
	page *motifs.Paging,
	revComp bool,
	useCache bool) (*motifs.MotifSearchResult, error) {
	return instance.Search(queries, datasets, page, revComp, useCache)
}

func BoolSearch(q string,
	datasets []string,
	page *motifs.Paging,

	revComp,
	useCache bool) (*motifs.MotifSearchResult, error) {
	return instance.BoolSearch(q, datasets, page, revComp, useCache)
}

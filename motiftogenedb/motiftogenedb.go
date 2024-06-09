package motiftogenedb

import (
	"sync"

	motiftogene "github.com/antonybholmes/go-motiftogene"
)

var instance *motiftogene.MotifToGeneDB
var once sync.Once

func InitCache(file string) *motiftogene.MotifToGeneDB {
	once.Do(func() {
		instance = motiftogene.NewMotifToGeneDB(file)
	})

	return instance
}

func GetInstance() *motiftogene.MotifToGeneDB {
	return instance
}

func Convert(search string) (*motiftogene.MotifToGene, error) {
	return instance.Convert(search)
}

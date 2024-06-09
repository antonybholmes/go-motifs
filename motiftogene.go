package motiftogene

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/antonybholmes/go-sys"
)

type MotifToGene struct {
	Databases []string `json:"db"`
	Genes     []string `json:"genes"`
}

type MotifToGeneMap map[string]MotifToGene

type MotifToGeneDB struct {
	db MotifToGeneMap
}

func NewMotifToGeneDB(file string) *MotifToGeneDB {
	jsonFile := sys.Must(os.Open(file))

	defer jsonFile.Close()

	byteValue, _ := io.ReadAll(jsonFile)

	var motifToGeneMap MotifToGeneMap

	json.Unmarshal(byteValue, &motifToGeneMap)

	return &MotifToGeneDB{db: motifToGeneMap}
}

func (motiftogenedb *MotifToGeneDB) Convert(search string) (*MotifToGene, error) {

	gene, ok := motiftogenedb.db[search]

	if !ok {
		return nil, fmt.Errorf("%s does not exist", search)
	}

	return &gene, nil

}

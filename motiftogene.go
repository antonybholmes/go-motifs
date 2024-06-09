package motiftogene

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/antonybholmes/go-sys"
)

const MOUSE_TO_HUMAN_EXACT_SQL = `SELECT human.gene_id, human.gene_symbol, human.aliases, human.entrez, human.refseq, human.ensembl
	FROM mouse_terms, conversion, human
 	WHERE LOWER(mouse_terms.term) = LOWER(?1) AND conversion.mouse_gene_id = mouse_terms.gene_id AND human.gene_id = conversion.human_gene_id`

const HUMAN_TO_MOUSE_EXACT_SQL = `SELECT mouse.gene_id, mouse.gene_symbol, mouse.aliases, mouse.entrez, mouse.refseq, mouse.ensembl
	FROM human_terms, conversion, mouse
	WHERE LOWER(human_terms.term) = LOWER(?1) AND conversion.human_gene_id = human_terms.gene_id AND mouse.gene_id = conversion.mouse_gene_id`

const MOUSE_TO_HUMAN_SQL = `SELECT human.gene_id, human.gene_symbol, human.aliases, human.entrez, human.refseq, human.ensembl
	FROM mouse_terms, conversion, human
 	WHERE mouse_terms.term LIKE ?1 AND conversion.mouse_gene_id = mouse_terms.gene_id AND human.gene_id = conversion.human_gene_id`

const HUMAN_TO_MOUSE_SQL = `SELECT mouse.gene_id, mouse.gene_symbol, mouse.aliases, mouse.entrez, mouse.refseq, mouse.ensembl
	FROM human_terms, conversion, mouse
	WHERE human_terms.term LIKE ?1 AND conversion.human_gene_id = human_terms.gene_id AND mouse.gene_id = conversion.mouse_gene_id`

const MOUSE_SQL = `SELECT mouse.gene_id, mouse.gene_symbol, mouse.aliases, mouse.entrez, mouse.refseq, mouse.ensembl
	FROM mouse_terms, mouse
 	WHERE mouse_terms.term LIKE ?1 AND mouse.gene_id = mouse_terms.gene_id`

const HUMAN_SQL = `SELECT human.gene_id, human.gene_symbol, human.aliases, human.entrez, human.refseq, human.ensembl
	FROM human_terms, human
 	WHERE human_terms.term LIKE ?1 AND human.gene_id = human_terms.gene_id`

const MOUSE_EXACT_SQL = `SELECT mouse.gene_id, mouse.gene_symbol, mouse.aliases, mouse.entrez, mouse.refseq, mouse.ensembl
	FROM mouse_terms, mouse
	WHERE LOWER(mouse_terms.term) = LOWER(?1) AND mouse.gene_id = mouse_terms.gene_id`

const HUMAN_EXACT_SQL = `SELECT human.gene_id, human.gene_symbol, human.aliases, human.entrez, human.refseq, human.ensembl
	FROM human_terms, human
	WHERE LOWER(human_terms.term) = LOWER(?1) AND human.gene_id = human_terms.gene_id`

const HUMAN_TAXONOMY_ID = 9606
const MOUSE_TAXONOMY_ID = 10090

const HUMAN_SPECIES = "human"
const MOUSE_SPECIES = "mouse"

type Taxonomy struct {
	Id      uint64 `json:"id"`
	Species string `json:"species"`
}

var HUMAN_TAX = Taxonomy{
	Id:      HUMAN_TAXONOMY_ID,
	Species: HUMAN_SPECIES,
}

var MOUSE_TAX = Taxonomy{
	Id:      MOUSE_TAXONOMY_ID,
	Species: MOUSE_SPECIES,
}

type BaseGene struct {
	Taxonomy Taxonomy `json:"taxonomy"`
	Id       string   `json:"id"`
}

type Gene struct {
	BaseGene
	Symbol  string   `json:"symbol"`
	Aliases []string `json:"aliases"`
	Entrez  int      `json:"entrez"`
	RefSeq  []string `json:"refseq"`
	Ensembl []string `json:"ensembl"`
}

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

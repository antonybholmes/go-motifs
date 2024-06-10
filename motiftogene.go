package motiftogene

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/antonybholmes/go-sys"
)

type MotifToGene struct {
	Motif   string   `json:"motif"`
	Sources []string `json:"sources"`
	Genes   []string `json:"genes"`
}

type MotifToGeneMap map[string]MotifToGene

type MotifToGeneDB struct {
	db *sql.DB //MotifToGeneMap
}

func NewMotifToGeneDB(file string) *MotifToGeneDB {
	// jsonFile := sys.Must(os.Open(file))

	// defer jsonFile.Close()

	// byteValue, _ := io.ReadAll(jsonFile)

	// var motifToGeneMap MotifToGeneMap

	// json.Unmarshal(byteValue, &motifToGeneMap)

	// return &MotifToGeneDB{db: motifToGeneMap}

	return &MotifToGeneDB{db: sys.Must(sql.Open("sqlite3", file))}
}

func (motiftogenedb *MotifToGeneDB) Convert(search string) (*MotifToGene, error) {

	var ret MotifToGene
	var sources string
	var genes string

	//log.Debug().Msgf("motif %s", search)

	err := motiftogenedb.db.QueryRow("SELECT motif, sources, genes FROM motifs WHERE motif LIKE ?1",
		fmt.Sprintf("%%%s%%", search)).Scan(&ret.Motif,
		&sources,
		&genes)

	if err != nil {
		//log.Debug().Msgf("motif %s", err)
		return nil, err
	}

	ret.Sources = strings.Split(sources, "|")
	ret.Genes = strings.Split(genes, "|")

	// ret.Databases = strings.Split(sources, "|")
	// ret.Genes = strings.Split(genes, "|")

	// for rows.Next() {
	// 	var gene Gene
	// 	gene.Entrez = -1
	// 	gene.Taxonomy = tax

	// gene, ok := motiftogenedb.db[search]

	// if !ok {
	// 	return nil, fmt.Errorf("%s does not exist", search)
	// }

	return &ret, nil

}

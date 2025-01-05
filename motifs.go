package motifs

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/antonybholmes/go-sys"
	_ "github.com/mattn/go-sqlite3"
)

const DATASETS_SQL = "SELECT DISTINCT motifs.dataset FROM motifs ORDER BY motifs.dataset"
const SEARCH_SQL = "SELECT motifs.public_id, motifs.dataset, motifs.motif_id, motifs.motif_name, motifs.genes, motifs.weights FROM motifs WHERE motif_id LIKE ?1 OR motif_name LIKE ?1"

type Motif struct {
	PublicId  string      `json:"publicId"`
	Dataset   string      `json:"dataset"`
	MotifId   string      `json:"motifId"`
	MotifName string      `json:"motifName"`
	Genes     []string    `json:"genes"`
	Weights   [][]float32 `json:"weights"`
}

type MotifToGeneMap map[string]Motif

type MotifDB struct {
	db   *sql.DB
	file string
}

func NewMotifDB(file string) *MotifDB {
	// jsonFile := sys.Must(os.Open(file))

	// defer jsonFile.Close()

	// byteValue, _ := io.ReadAll(jsonFile)

	// var motifToGeneMap MotifToGeneMap

	// json.Unmarshal(byteValue, &motifToGeneMap)

	// return &MotifToGeneDB{db: motifToGeneMap}

	return &MotifDB{file: file, db: sys.Must(sql.Open("sqlite3", file))}
}

func (motifdb *MotifDB) Datasets() ([]string, error) {

	var ret []string = make([]string, 0, 10)

	var dataset string

	//log.Debug().Msgf("motif %s", search)

	rows, err := motifdb.db.Query(DATASETS_SQL)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {

		err := rows.Scan(&dataset)

		if err != nil {
			return nil, err
		}

		ret = append(ret, dataset)
	}

	return ret, nil

}

func (motifdb *MotifDB) Search(search string, reverse bool, complement bool) ([]*Motif, error) {

	var ret []*Motif = make([]*Motif, 0, 20)

	var genes string
	var weights string

	//log.Debug().Msgf("motif %s", search)

	rows, err := motifdb.db.Query(SEARCH_SQL,
		fmt.Sprintf("%%%s%%", search))

	if err != nil {
		// return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var motif Motif

		err := rows.Scan(&motif.PublicId,
			&motif.Dataset,
			&motif.MotifId,
			&motif.MotifName,
			&genes,
			&weights)

		if err != nil {
			//log.Debug().Msgf("motif %s", err)
			return nil, err
		}

		motif.Genes = strings.Split(genes, ",")

		json.Unmarshal([]byte(weights), &motif.Weights)

		if reverse {
			slices.Reverse(motif.Weights)
		}

		if complement {
			for _, pw := range motif.Weights {
				slices.Reverse(pw)
			}
		}

		ret = append(ret, &motif)
	}

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

	return ret, nil

}

package motifs

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/antonybholmes/go-sys"
)

type Motif struct {
	Dataset   string      `json:"dataset"`
	MotifId   string      `json:"motifId"`
	MotifName string      `json:"motifName"`
	Genes     []string    `json:"genes"`
	Weights   [][]float32 `json:"weights"`
}

type MotifToGeneMap map[string]Motif

type MotifDB struct {
	file string
	db   *sql.DB //MotifToGeneMap
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

func (motiftogenedb *MotifDB) Convert(search string) (*Motif, error) {

	var ret Motif

	var genes string
	var weights string

	//log.Debug().Msgf("motif %s", search)

	err := motiftogenedb.db.QueryRow("SELECT dataset, motif_id, motif_name, genes FROM motifs WHERE motif LIKE ?1",
		fmt.Sprintf("%%%s%%", search)).Scan(&ret.Dataset,
		&ret.MotifId,
		&ret.MotifName,
		&genes,
		&weights)

	if err != nil {
		//log.Debug().Msgf("motif %s", err)
		return nil, err
	}

	ret.Genes = strings.Split(genes, ",")

	json.Unmarshal([]byte(weights), &ret.Weights)

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

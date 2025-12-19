package motifs

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/antonybholmes/go-sys"
)

type (
	Motif struct {
		Id        string      `json:"id"`
		Dataset   string      `json:"dataset"`
		MotifId   string      `json:"motifId"`
		MotifName string      `json:"motifName"`
		Genes     []string    `json:"genes"`
		Weights   [][]float32 `json:"weights"`
	}

	MotifToGeneMap map[string]Motif

	MotifDB struct {
		db   *sql.DB
		file string
	}
)

const (
	DatasetsSql = `SELECT DISTINCT 
		motifs.dataset 
		FROM motifs 
		ORDER BY motifs.dataset`

	// SearchSql = `SELECT 
	// 	m.id, m.dataset, m.motif_id, m.motif_name, m.genes
	// 	FROM motifs m
	// 	WHERE m.id = :id OR m.motif_id LIKE :q OR m.motif_name LIKE :q`

	// SearchFtsSql = `SELECT 
	// 	motifs.id, motifs.dataset, motifs.motif_id, motifs.motif_name, motifs.genes
	// 	FROM motifs_fts 
	// 	JOIN motifs ON motifs.rowid = motifs_fts.rowid
	// 	WHERE motifs_fts MATCH :q`

	// search for either exact id or partial match on
	// either motif_id or motif_name. We limit to 100
	// for performance reasons and it seems unlikely that
	// a specific search will yield 100 results
	SearchFtsSql = `SELECT 
		m.motifs.id, m.motifs.dataset, m.motifs.motif_id, m.motifs.motif_name, m.motifs.genes
		FROM motifs m
		LEFT JOIN motifs_fts f ON m.rowid = f.rowid
		WHERE m.id = :id OR f MATCH :q
		ORDER BY m.dataset, m.motif_id ASC
		LIMIT 100`

	WeightsSql = `SELECT 
		w.position, w.a, w.c, w.g, w.t 
		FROM weights w
		WHERE w.motif_id = :id 
		ORDER BY w.position ASC`
)

func NewMotifDB(file string) *MotifDB {
	return &MotifDB{file: file, db: sys.Must(sql.Open(sys.Sqlite3DB, file))}
}

func (motifdb *MotifDB) Datasets() ([]string, error) {
	var ret []string = make([]string, 0, 20)

	var dataset string

	//log.Debug().Msgf("motif %s", search)

	rows, err := motifdb.db.Query(DatasetsSql)

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

	//log.Debug().Msgf("motif %s", search)

	// rows, err := motifdb.db.Query(SearchSql,
	// 	sql.Named("id", search),
	// 	sql.Named("q", fmt.Sprintf("%%%s%%", search)))

	rows, err := motifdb.db.Query(SearchFtsSql,
		sql.Named("id", search),
		sql.Named("q",  search+"*"))

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var motif Motif

		err := rows.Scan(&motif.Id,
			&motif.Dataset,
			&motif.MotifId,
			&motif.MotifName,
			&genes)

		if err != nil {
			return nil, err
		}

		motif.Genes = strings.Split(genes, "|")

		motif.Weights = make([][]float32, 0, 20)

		weightRows, err := motifdb.db.Query(WeightsSql,
			sql.Named("id", motif.Id))

		if err != nil {
			return nil, err
		}

		var position int
		var a, c, g, t float32

		for weightRows.Next() {
			err := weightRows.Scan(&position, &a, &c, &g, &t)

			if err != nil {
				return nil, err
			}

			motif.Weights = append(motif.Weights, []float32{a, c, g, t})
		}

		weightRows.Close()

		// weight are stored as a string of floats in database
		// which we can parse as json
		//json.Unmarshal([]byte(weights), &motif.Weights)

		// reverse position order
		if reverse {
			slices.Reverse(motif.Weights)
		}

		// complement weights switch A<->T and C<->G
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

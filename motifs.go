package motifs

import (
	"database/sql"
	"slices"
	"strings"

	basemath "github.com/antonybholmes/go-math"
	"github.com/antonybholmes/go-sys"
)

type (
	Dataset struct {
		Id         string `json:"id"`
		Name       string `json:"name"`
		MotifCount int    `json:"motifCount"`
	}

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

	MotifSearchResult struct {
		Motifs   []*Motif `json:"motifs"`
		Total    int      `json:"total"`
		Page     int      `json:"page"`
		PageSize int      `json:"pageSize"`
	}
)

const (
	MinSearchLen = 3
	MinPageSize  = 10
	MaxPageSize  = 100

	// DatasetsSql = `SELECT DISTINCT
	// 	motifs.dataset
	// 	FROM motifs
	// 	ORDER BY motifs.dataset`

	DatasetsSql = `SELECT DISTINCT
		d.id, 
		d.name,
		COUNT (m.id) as motif_count
		FROM motifs m
		JOIN datasets d ON m.dataset_id = d.id
		GROUP BY d.id
		ORDER BY d.name ASC`

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

	SearchNumRecordsFtsSql = `SELECT COUNT(m.id) AS total FROM (
		-- Direct match on motifs.id
		SELECT m.id 
		FROM motifs m 
		WHERE m.id = :id

		UNION
		
		-- FTS search
		SELECT m.id
		FROM motifs m
		JOIN motifs_fts ON m.rowid = motifs_fts.rowid
		WHERE motifs_fts MATCH :q

		-- Also allow searching by dataset

		UNION

		-- search datasets
		SELECT m.id
		FROM motifs m
		JOIN datasets d ON m.dataset_id = d.id
		WHERE d.id = :id

		UNION
		
		-- FTS search
		SELECT m.id
		FROM motifs m
		JOIN datasets d ON m.dataset_id = d.id
		JOIN datasets_fts ON d.rowid = datasets_fts.rowid
		WHERE datasets_fts MATCH :q
	) AS m;`

	// SearchFtsSql = `SELECT
	// 	m.motifs.id, m.motifs.dataset, m.motifs.motif_id, m.motifs.motif_name, m.motifs.genes
	// 	FROM motifs m
	// 	LEFT JOIN motifs_fts f ON m.rowid = f.rowid
	// 	WHERE m.id = :id OR f MATCH :q
	// 	ORDER BY m.dataset, m.motif_id ASC
	// 	LIMIT :limit
	// 	OFFSET :offset`

	SearchFtsSql = `SELECT m.id, m.dataset, m.motif_id, m.motif_name, m.genes FROM (
			-- Direct match on motifs.id
			SELECT m.id, d.name as dataset, m.motif_id, m.motif_name, m.genes 
			FROM motifs m
			JOIN datasets d ON m.dataset_id = d.id
			WHERE m.id = :id
			
			UNION
			
			-- FTS search
			SELECT m.id, d.name as dataset, m.motif_id, m.motif_name, m.genes 
			FROM motifs m
			JOIN datasets d ON m.dataset_id = d.id
			JOIN motifs_fts ON m.rowid = motifs_fts.rowid
			WHERE motifs_fts MATCH :q

			-- Also allow searching by dataset
			
			UNION

			-- search datasets
			SELECT m.id, d.name as dataset, m.motif_id, m.motif_name, m.genes 
			FROM motifs m
			JOIN datasets d ON m.dataset_id = d.id
			WHERE d.id = :id
			
			UNION
			
			-- FTS search
			SELECT m.id, d.name as dataset, m.motif_id, m.motif_name, m.genes 
			FROM motifs m
			JOIN datasets d ON m.dataset_id = d.id
			JOIN datasets_fts ON d.rowid = datasets_fts.rowid
			WHERE datasets_fts MATCH :q

		) AS m
		ORDER BY m.dataset, m.motif_id ASC
		LIMIT :limit 
		OFFSET :offset;`

	WeightsSql = `SELECT 
		w.position, w.a, w.c, w.g, w.t 
		FROM weights w
		WHERE w.motif_id = :id 
		ORDER BY w.position ASC`
)

func NewMotifDB(file string) *MotifDB {
	return &MotifDB{file: file, db: sys.Must(sql.Open(sys.Sqlite3DB, file))}
}

func (motifdb *MotifDB) Datasets() ([]*Dataset, error) {
	var datasets []*Dataset = make([]*Dataset, 0, 20)

	//log.Debug().Msgf("motif %s", search)

	rows, err := motifdb.db.Query(DatasetsSql)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var dataset Dataset

		err := rows.Scan(&dataset.Id, &dataset.Name, &dataset.MotifCount)

		if err != nil {
			return nil, err
		}

		datasets = append(datasets, &dataset)
	}

	return datasets, nil

}

func (motifdb *MotifDB) Search(search string, page int, pageSize int, reverse bool, complement bool) (*MotifSearchResult, error) {
	// clamp page number
	page = basemath.Max(page, 1)

	// clamp page size
	pageSize = basemath.Min(basemath.Max(pageSize, MinPageSize), MaxPageSize)

	result := MotifSearchResult{Total: 0, Page: page, PageSize: pageSize, Motifs: make([]*Motif, 0, 20)}

	var genes string

	//log.Debug().Msgf("motif %s", search)

	// rows, err := motifdb.db.Query(SearchSql,
	// 	sql.Named("id", search),
	// 	sql.Named("q", fmt.Sprintf("%%%s%%", search)))

	tx, err := motifdb.db.Begin()

	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	// for full text search, we append wildcard to search term
	// to allow partial matches
	q := search + "*"

	row := tx.QueryRow(SearchNumRecordsFtsSql, sql.Named("id", search),
		sql.Named("q", q))

	// records in total

	err = row.Scan(&result.Total)

	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(SearchFtsSql,
		sql.Named("id", search),
		sql.Named("q", q),
		sql.Named("offset", pageSize*(page-1)),
		sql.Named("limit", pageSize),
	)

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

		weightRows, err := tx.Query(WeightsSql,
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

		result.Motifs = append(result.Motifs, &motif)
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

	return &result, nil

}

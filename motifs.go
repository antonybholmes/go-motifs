package motifs

import (
	"database/sql"
	"fmt"

	"slices"
	"strings"

	basemath "github.com/antonybholmes/go-math"
	"github.com/antonybholmes/go-sys"
	"github.com/antonybholmes/go-sys/log"
	"github.com/antonybholmes/go-sys/query"
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

	TempPatternSql = `CREATE TEMP TABLE IF NOT EXISTS temp_patterns (
		id TEXT PRIMARY KEY,
		query TEXT,
		UNIQUE(id, query)
	);`

	InsertTempPatternSql = `INSERT INTO temp_patterns (id, query) VALUES (:id, :query) ON CONFLICT DO NOTHING;`

	//DropTempPatternSql = `DROP TABLE IF EXISTS temp_pattern;`

	DatasetsSql = `SELECT DISTINCT
		d.id, 
		d.name,
		COUNT (m.id) as total
		FROM motifs m
		JOIN datasets d ON m.dataset_id = d.id
		GROUP BY d.id
		ORDER BY d.name ASC`

	// SearchNumRecordsSql = `SELECT COUNT(m.id) AS total FROM (
	// 		-- Direct match on motifs.id
	// 		SELECT m.id
	// 		FROM motifs m
	// 		WHERE m.id = :id OR m.motif_id LIKE :q OR m.motif_name LIKE :q

	// 		UNION

	// 		-- search datasets
	// 		SELECT m.id
	// 		FROM motifs m
	// 		JOIN datasets d ON m.dataset_id = d.id
	// 		WHERE d.id = :id OR d.name LIKE :q
	// 	) AS m;`

	SearchNumRecordsSql = `SELECT COUNT(m.id) AS total FROM (
			-- Direct match on motifs.id
			SELECT m.id 
			FROM motifs m
			JOIN temp_patterns tp ON 
				m.id = tp.id OR	
				m.motif_id LIKE tp.query OR
				m.motif_name LIKE tp.query 
				
		 
			UNION

			-- search datasets
			SELECT m.id
			FROM motifs m
			JOIN datasets d ON m.dataset_id = d.id
			JOIN temp_patterns tp ON 
				d.id = tp.id OR 
				d.name LIKE tp.query
		) AS m;`

	// SearchSql = `SELECT
	// 	m.id, m.dataset, m.motif_id, m.motif_name, m.genes
	// 	FROM motifs m
	// 	WHERE m.id = :id OR m.motif_id LIKE :q OR m.motif_name LIKE :q`

	// SearchSql = `SELECT m.id, m.dataset, m.motif_id, m.motif_name, m.genes FROM (
	// 		-- Direct match on motifs.id
	// 		SELECT m.id, d.name as dataset, m.motif_id, m.motif_name, m.genes
	// 		FROM motifs m
	// 		JOIN datasets d ON m.dataset_id = d.id
	// 		WHERE m.id = :id OR m.motif_id LIKE :q OR m.motif_name LIKE :q

	// 		UNION

	// 		-- search datasets
	// 		SELECT m.id, d.name as dataset, m.motif_id, m.motif_name, m.genes
	// 		FROM motifs m
	// 		JOIN datasets d ON m.dataset_id = d.id
	// 		WHERE d.id = :id OR d.name LIKE :q
	// 	) AS m
	// 	ORDER BY m.dataset, m.motif_id ASC
	// 	LIMIT :limit
	// 	OFFSET :offset;`

	SearchSql = `SELECT m.id, m.dataset, m.motif_id, m.motif_name, m.genes FROM (
			-- Direct match on motifs.id
			SELECT m.id, d.name as dataset, m.motif_id, m.motif_name, m.genes
			FROM motifs m
			JOIN datasets d ON m.dataset_id = d.id
			JOIN temp_patterns tp ON 
				m.id = tp.id OR
				m.motif_id LIKE tp.query OR 
				m.motif_name LIKE tp.query
				
			 
			
			UNION

			-- search datasets
			SELECT m.id, d.name as dataset, m.motif_id, m.motif_name, m.genes
			FROM motifs m
			JOIN datasets d ON m.dataset_id = d.id
			JOIN temp_patterns tp ON 
				d.id = tp.id OR 
				d.name LIKE tp.query
		) AS m
		ORDER BY m.dataset, m.motif_id ASC
		LIMIT :limit 
		OFFSET :offset;`

	// search for either exact id or partial match on
	// either motif_id or motif_name. We limit to 100
	// for performance reasons and it seems unlikely that
	// a specific search will yield 100 results

	// SearchNumRecordsFtsSql = `SELECT COUNT(m.id) AS total FROM (
	// 	-- Direct match on motifs.id
	// 	SELECT m.id
	// 	FROM motifs m
	// 	WHERE m.id = :id

	// 	UNION

	// 	-- FTS search
	// 	SELECT m.id
	// 	FROM motifs m
	// 	JOIN motifs_fts ON m.rowid = motifs_fts.rowid
	// 	WHERE motifs_fts MATCH :q

	// 	-- Also allow searching by dataset

	// 	UNION

	// 	-- search datasets
	// 	SELECT m.id
	// 	FROM motifs m
	// 	JOIN datasets d ON m.dataset_id = d.id
	// 	WHERE d.id = :id

	// 	UNION

	// 	-- FTS search
	// 	SELECT m.id
	// 	FROM motifs m
	// 	JOIN datasets d ON m.dataset_id = d.id
	// 	JOIN datasets_fts ON d.rowid = datasets_fts.rowid
	// 	WHERE datasets_fts MATCH :q
	// ) AS m;`

	// SearchFtsSql = `SELECT m.id, m.dataset, m.motif_id, m.motif_name, m.genes FROM (
	// 		-- Direct match on motifs.id
	// 		SELECT m.id, d.name as dataset, m.motif_id, m.motif_name, m.genes
	// 		FROM motifs m
	// 		JOIN datasets d ON m.dataset_id = d.id
	// 		WHERE m.id = :id

	// 		UNION

	// 		-- FTS search
	// 		SELECT m.id, d.name as dataset, m.motif_id, m.motif_name, m.genes
	// 		FROM motifs m
	// 		JOIN datasets d ON m.dataset_id = d.id
	// 		JOIN motifs_fts ON m.rowid = motifs_fts.rowid
	// 		WHERE motifs_fts MATCH :q

	// 		-- Also allow searching by dataset

	// 		UNION

	// 		-- search datasets
	// 		SELECT m.id, d.name as dataset, m.motif_id, m.motif_name, m.genes
	// 		FROM motifs m
	// 		JOIN datasets d ON m.dataset_id = d.id
	// 		WHERE d.id = :id

	// 		UNION

	// 		-- FTS search
	// 		SELECT m.id, d.name as dataset, m.motif_id, m.motif_name, m.genes
	// 		FROM motifs m
	// 		JOIN datasets d ON m.dataset_id = d.id
	// 		JOIN datasets_fts ON d.rowid = datasets_fts.rowid
	// 		WHERE datasets_fts MATCH :q

	// 	) AS m
	// 	ORDER BY m.dataset, m.motif_id ASC
	// 	LIMIT :limit
	// 	OFFSET :offset;`

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

func (motifdb *MotifDB) Search(queries []string, page int, pageSize int, reverse bool, complement bool) (*MotifSearchResult, error) {
	// clamp page number
	page = basemath.Max(page, 1)

	// clamp page size
	pageSize = basemath.Min(basemath.Max(pageSize, MinPageSize), MaxPageSize)

	result := MotifSearchResult{Total: 0,
		Page:     page,
		PageSize: pageSize,
		Motifs:   make([]*Motif, 0, 20)}

	log.Debug().Msgf("motif %v", queries)

	// rows, err := motifdb.db.Query(SearchSql,
	// 	sql.Named("id", search),
	// 	sql.Named("q", fmt.Sprintf("%%%s%%", search)))

	tx, err := motifdb.db.Begin()

	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	//_, err = tx.Exec(DropTempPatternSql)

	//if err != nil {
	//	return nil, err
	//}

	log.Debug().Msgf("creating temp table")

	_, err = tx.Exec(TempPatternSql)

	if err != nil {
		log.Debug().Msgf("motif temp table %s", err)
		return nil, err
	}

	stmt, err := tx.Prepare(InsertTempPatternSql)

	if err != nil {
		log.Debug().Msgf("motif ddd %s", err)
		return nil, err
	}

	defer stmt.Close()

	for _, q := range queries {
		_, err := stmt.Exec(sql.Named("id", q),
			sql.Named("query", q+"%"))

		if err != nil {
			log.Debug().Msgf("motif insert temp %s", err)
			return nil, err
		}
	}

	log.Debug().Msgf("queries inserted")

	// for full text search, we append wildcard to search term
	// to allow partial matches
	// q := search + "*"

	// original version without FTS prefix matching
	//q := search + "%"

	// row := tx.QueryRow(SearchNumRecordsSql,
	// 	sql.Named("id", search),
	// 	sql.Named("q", q))

	row := tx.QueryRow(SearchNumRecordsSql)

	// records in total

	err = row.Scan(&result.Total)

	if err != nil {
		return nil, err
	}

	// rows, err := tx.Query(SearchSql,
	// 	sql.Named("id", search),
	// 	sql.Named("q", q),
	// 	sql.Named("offset", pageSize*(page-1)),
	// 	sql.Named("limit", pageSize),
	// )

	rows, err := tx.Query(SearchSql,
		sql.Named("offset", pageSize*(page-1)),
		sql.Named("limit", pageSize),
	)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	return processRows(tx, rows, reverse, complement, &result)
}

// More complex boolean search
func (motifdb *MotifDB) BoolSearch(q string,
	page int,
	pageSize int,
	reverse bool,
	complement bool) (*MotifSearchResult, error) {

	// clamp page number
	page = basemath.Max(page, 1)

	// clamp page size
	pageSize = basemath.Min(basemath.Max(pageSize, MinPageSize), MaxPageSize)

	result := MotifSearchResult{Total: 0,
		Page:     page,
		PageSize: pageSize,
		Motifs:   make([]*Motif, 0, 20)}

	log.Debug().Msgf("motif %v", q)

	// rows, err := motifdb.db.Query(SearchSql,
	// 	sql.Named("id", search),
	// 	sql.Named("q", fmt.Sprintf("%%%s%%", search)))

	tx, err := motifdb.db.Begin()

	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	tree, err := query.SqlBoolTree(q)

	if err != nil {
		return nil, err
	}

	motifIdWhere, err := query.SqlBoolQueryFromTree(tree, func(placeholderIndex int, matchType query.MatchType, not bool) string {
		// for slqlite
		ph := fmt.Sprintf("?%d", placeholderIndex)

		equalOp := " = "
		if not {
			equalOp = " != "
		}

		// we use like even for exact matches to allow for case insensitivity
		return "m.id" + equalOp + ph //] fmt.Sprintf("(gex.gene_symbol LIKE %s OR gex.ensembl_id LIKE %s)", ph, ph)
	})

	if err != nil {
		return nil, err
	}

	placeholderOffset := len(motifIdWhere.Args)

	motifNameWhere, err := query.SqlBoolQueryFromTree(tree, func(placeholderIndex int, matchType query.MatchType, not bool) string {
		// for slqlite
		ph := fmt.Sprintf("?%d", placeholderOffset+placeholderIndex)

		equalOp := " LIKE "
		if not {
			equalOp = " NOT LIKE "
		}

		// we use like even for exact matches to allow for case insensitivity
		return "(m.motif_id" + equalOp + ph + " OR m.motif_name" + equalOp + ph + ")"
	})

	if err != nil {
		return nil, err
	}

	placeholderOffset += len(motifNameWhere.Args)

	datasetIdWhere, err := query.SqlBoolQueryFromTree(tree, func(placeholderIndex int, matchType query.MatchType, not bool) string {
		// for slqlite
		ph := fmt.Sprintf("?%d", placeholderOffset+placeholderIndex)

		equalOp := " = "
		if not {
			equalOp = " != "
		}

		// we use like even for exact matches to allow for case insensitivity
		return "d.id" + equalOp + ph //] fmt.Sprintf("(gex.gene_symbol LIKE %s OR gex.ensembl_id LIKE %s)", ph, ph)
	})

	if err != nil {
		return nil, err
	}

	placeholderOffset += len(datasetIdWhere.Args)

	datasetNameWhere, err := query.SqlBoolQueryFromTree(tree, func(placeholderIndex int, matchType query.MatchType, not bool) string {
		// for slqlite
		ph := fmt.Sprintf("?%d", placeholderOffset+placeholderIndex)

		equalOp := " LIKE "
		if not {
			equalOp = " NOT LIKE "
		}

		// we use like even for exact matches to allow for case insensitivity
		return "d.name" + equalOp + ph
	})

	if err != nil {
		return nil, err
	}

	countSql := fmt.Sprintf(`SELECT COUNT(m.id) AS total FROM (
			-- Direct match on motifs.id
			SELECT m.id
			FROM motifs m 
			WHERE (%s) OR (%s) 
			UNION

			-- search datasets
			SELECT m.id
			FROM motifs m
			JOIN datasets d ON m.dataset_id = d.id 
			WHERE (%s) OR (%s) 
		) AS m;`,
		motifIdWhere.Sql,
		motifNameWhere.Sql,
		datasetIdWhere.Sql,
		datasetNameWhere.Sql)

	row := tx.QueryRow(countSql)

	// records in total

	err = row.Scan(&result.Total)

	if err != nil {
		return nil, err
	}

	// easier to code limit and offset directly into sql here
	// than via named parameters due to the dynamic nature of the query
	// and they are vetted ints so there is no sql injection risk
	searchSql := fmt.Sprintf(`SELECT m.id, m.dataset, m.motif_id, m.motif_name, m.genes FROM (
			-- Direct match on motifs.id
			SELECT m.id, d.name as dataset, m.motif_id, m.motif_name, m.genes
			FROM motifs m 
			WHERE (%s) OR (%s) 
			UNION

			-- search datasets
			SELECT m.id, d.name as dataset, m.motif_id, m.motif_name, m.genes
			FROM motifs m
			JOIN datasets d ON m.dataset_id = d.id `+
		`WHERE (%s) OR (%s) 
		) AS m
		ORDER BY 
		m.dataset, m.motif_id ASC 
		LIMIT %d OFFSET %d`,
		motifIdWhere.Sql,
		motifNameWhere.Sql,
		datasetIdWhere.Sql,
		datasetNameWhere.Sql,
		pageSize,
		pageSize*(page-1))

	log.Debug().Msgf("search sql: %s", searchSql)

	rows, err := tx.Query(searchSql, motifIdWhere.Args...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	return processRows(tx, rows, reverse, complement, &result)
}

func processRows(tx *sql.Tx, rows *sql.Rows, reverse bool, complement bool, result *MotifSearchResult) (*MotifSearchResult, error) {

	var genes string

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

	return result, nil

}

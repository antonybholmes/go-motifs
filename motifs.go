package motifs

import (
	"database/sql"
	"time"

	"slices"
	"strings"

	"github.com/antonybholmes/go-sys"
	"github.com/antonybholmes/go-sys/log"
	"github.com/antonybholmes/go-sys/query"
)

type (
	Paging struct {
		Page     int `json:"page"`
		Pages    int `json:"pages,omitempty"`
		PageSize int `json:"pageSize"`
	}

	// Dataset struct {
	// 	DatasetCount
	// 	Name string `json:"name"`
	// }

	Dataset struct {
		sys.Entity
		MotifCount int `json:"motifCount"`
	}

	Motif struct {
		sys.Entity
		Dataset *sys.Entity `json:"dataset"`
		MotifId string      `json:"motifId"`

		Genes   []string    `json:"genes"`
		Weights [][]float64 `json:"weights"`
	}

	MotifToGeneMap map[string]Motif

	MotifDB struct {
		db *sql.DB
		//cache *expirable.LRU[string, any]
		file string
	}

	MotifSearchResult struct {
		Paging *Paging  `json:"paging"`
		Motifs []*Motif `json:"motifs"`
		Total  int      `json:"total"`
	}
)

const (
	CacheSize   = 100
	CacheExpiry = time.Hour

	MinSearchLen = 3
	MinPageSize  = 10
	MaxPageSize  = 100

	// DatasetsSql = `SELECT DISTINCT
	// 	motifs.dataset
	// 	FROM motifs
	// 	ORDER BY motifs.dataset`

	TempQueriesTableSql = `CREATE TEMP TABLE IF NOT EXISTS temp_queries (
		id TEXT PRIMARY KEY,
		query TEXT,
		UNIQUE(id, query)
	);`

	InsertTempQueriesSql = `INSERT INTO temp_queries (id, query) VALUES (:id, :query) ON CONFLICT DO NOTHING;`

	TempDatasetTableSql = `CREATE TEMP TABLE IF NOT EXISTS temp_datasets (id TEXT PRIMARY KEY);`

	InsertTempDatasetSql = `INSERT INTO temp_datasets (id) VALUES (:id) ON CONFLICT DO NOTHING;`

	//DropTempPatternSql = `DROP TABLE IF EXISTS temp_pattern;`

	DatasetsSql = `SELECT DISTINCT
		d.public_id, 
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

	SearchNumRecordsSql = `SELECT DISTINCT 
		m.dataset_public_id,
		m.dataset_name,
		COUNT(m.id) AS total 
		FROM (
			-- Direct match on motifs.id
			SELECT 
			d.public_id AS dataset_public_id,
			d.name AS dataset_name, 
			m.id
			FROM motifs m
			JOIN datasets d ON m.dataset_id = d.id
			JOIN temp_datasets td ON d.public_id = td.id
			JOIN temp_queries tp ON 
				m.public_id = tp.id OR	
				m.motif_id LIKE tp.query OR
				m.motif_name LIKE tp.query 
			
			UNION

			-- search datasets
			SELECT 
			d.public_id AS dataset_public_id,
			d.name AS dataset_name, 
			m.id
			FROM motifs m
			JOIN datasets d ON m.dataset_id = d.id
			JOIN temp_datasets td ON d.public_id = td.id
			JOIN temp_queries tp ON 
				d.public_id = tp.id OR 
				d.name LIKE tp.query
		) AS m
		GROUP BY m.dataset_public_id;`

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

	SearchSql = `SELECT 
		m.dataset_public_id,
		m.dataset_name,
		m.motif_public_id,
		m.motif_id, 
		m.motif_name, 
		m.genes 
		FROM (
			-- Direct match on motifs.id
			SELECT 
			d.public_id AS dataset_public_id,
			d.name AS dataset_name,
			m.public_id AS motif_public_id, 
			m.motif_id, 
			m.motif_name, 
			m.genes
			FROM motifs m
			JOIN datasets d ON m.dataset_id = d.id
			JOIN temp_datasets td ON d.public_id = td.id
			JOIN temp_queries tp ON 
				m.public_id = tp.id OR
				m.motif_id LIKE tp.query OR 
				m.motif_name LIKE tp.query

			UNION

			-- search datasets
			SELECT 
			d.public_id AS dataset_public_id,
			d.name AS dataset_name,
			m.public_id AS motif_public_id, 
			m.motif_id, 
			m.motif_name,
			m.genes
			FROM motifs m
			JOIN datasets d ON m.dataset_id = d.id
			JOIN temp_datasets td ON d.public_id = td.id
			JOIN temp_queries tp ON 
				d.public_id = tp.id OR 
				d.name LIKE tp.query
			
		) AS m
		ORDER BY 
			m.dataset_public_id, 
			m.motif_id
		LIMIT :limit 
		OFFSET :offset;`

	BoolCountSql = `SELECT
		m.public_dataset_id,
		COUNT(m.id) AS total 
		FROM (
			-- Direct match on motifs.id
			SELECT 
			d.public_id AS public_dataset_id,
			m.id
			FROM motifs m
			JOIN datasets d ON m.dataset_id = d.id
			JOIN temp_datasets td ON d.public_id = td.id
			WHERE <<MOTIFS>>

			UNION

			-- search datasets
			SELECT 
			d.public_id AS public_dataset_id,
			d.name AS dataset_name,
			m.id
			FROM motifs m
			JOIN datasets d ON m.dataset_id = d.id
			JOIN temp_datasets td ON d.public_id = td.id
			WHERE <<DATASETS>>
		) AS m
		GROUP BY m.public_dataset_id;`

	BoolSearchSql = `SELECT 
		m.dataset_public_id,
		m.dataset_name,
		m.motif_public_id,
		m.motif_id, 
		m.motif_name, 
		m.genes 
		FROM (
			-- Direct match on motifs.id
			SELECT 
			d.public_id AS dataset_public_id,
			d.name AS dataset_name,
			m.public_id AS motif_public_id,
			m.motif_id, 
			m.motif_name, 
			m.genes
			FROM motifs m 
			JOIN datasets d ON m.dataset_id = d.id
			JOIN temp_datasets td ON d.public_id = td.id
			WHERE <<MOTIFS>>

			UNION

			-- search datasets
			SELECT 
			d.public_id AS dataset_public_id,
			d.name AS dataset_name,
			m.public_id motif_public_id, 
			m.motif_id, 
			m.motif_name, 
			m.genes
			FROM motifs m
			JOIN datasets d ON m.dataset_id = d.id
			JOIN temp_datasets td ON d.public_id = td.id
			WHERE <<DATASETS>>
		) AS m
		ORDER BY 
			m.public_dataset_id, 
			m.motif_id
		LIMIT :limit 
		OFFSET :offset;`

	WeightsSql = `SELECT 
		w.a, 
		w.c, 
		w.g, 
		w.t 
		FROM weights w
		JOIN motifs m ON w.motif_id = m.id
		WHERE m.public_id = :id 
		ORDER BY w.id`
)

func NewMotifDB(file string) *MotifDB {
	return &MotifDB{file: file,
		//cache: expirable.NewLRU[string, any](CacheSize, nil, CacheExpiry),
		db: sys.Must(sql.Open(sys.Sqlite3DB, file))}
}

func (mdb *MotifDB) Datasets() ([]*Dataset, error) {

	// if cached, found := mdb.cache.Get("datasets"); found {
	// 	log.Debug().Msgf("motif cache hit for datasets")
	// 	return cached.([]*Dataset), nil
	// }

	datasets := make([]*Dataset, 0, 20)

	//log.Debug().Msgf("motif %s", search)

	rows, err := mdb.db.Query(DatasetsSql)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var dataset Dataset

		err := rows.Scan(&dataset.PublicId, &dataset.Name, &dataset.MotifCount)

		if err != nil {
			return nil, err
		}

		datasets = append(datasets, &dataset)
	}

	// if useCache {
	// 	mdb.cache.Add("datasets", datasets)
	// }

	return datasets, nil
}

func (mdb *MotifDB) Search(queries []string,
	datasets []string,
	paging *Paging,
	revComp bool) (*MotifSearchResult, error) {
	// clamp page number
	paging.Page = max(paging.Page, 1)

	// clamp page size
	paging.PageSize = min(max(paging.PageSize, MinPageSize), MaxPageSize)

	// key := fmt.Sprintf("q:%s:d:%s:p:%d:ps:%d:rev:%t",
	// 	strings.Join(queries, ","),
	// 	strings.Join(datasets, ","),
	// 	page.Page,
	// 	page.PageSize,
	// 	revComp)

	// if cached, found := mdb.cache.Get(key); found {
	// 	log.Debug().Msgf("motif cache hit for key %s", key)
	// 	return cached.(*MotifSearchResult), nil
	// }

	result := MotifSearchResult{Total: 0,
		Paging: paging,

		Motifs: make([]*Motif, 0, 20)}

	log.Debug().Msgf("motif %v", queries)

	// rows, err := mdb.db.Query(SearchSql,
	// 	sql.Named("id", search),
	// 	sql.Named("q", fmt.Sprintf("%%%s%%", search)))

	tx, err := mdb.db.Begin()

	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	log.Debug().Msgf("creating temp table")

	_, err = tx.Exec(TempQueriesTableSql)

	if err != nil {
		return nil, err
	}

	stmt, err := tx.Prepare(InsertTempQueriesSql)

	if err != nil {
		return nil, err
	}

	for _, q := range queries {
		_, err := stmt.Exec(sql.Named("id", q),
			sql.Named("query", q+"%"))

		if err != nil {
			log.Debug().Msgf("motif insert temp %s", err)
			return nil, err
		}
	}

	stmt.Close()

	err = addTempDatasets(tx, datasets)

	if err != nil {
		return nil, err
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

	rows, err := tx.Query(SearchNumRecordsSql)

	// records in total

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var dataset Dataset

		err := rows.Scan(&dataset.PublicId, &dataset.Name, &dataset.MotifCount)

		if err != nil {
			return nil, err
		}

		result.Total += dataset.MotifCount
	}

	log.Debug().Msgf("total motifs found: %d", result.Total)

	paging.Pages = (result.Total + paging.PageSize - 1) / paging.PageSize

	// rows, err := tx.Query(SearchSql,
	// 	sql.Named("id", search),
	// 	sql.Named("q", q),
	// 	sql.Named("offset", pageSize*(page-1)),
	// 	sql.Named("limit", pageSize),
	// )

	rows, err = tx.Query(SearchSql,
		sql.Named("offset", paging.PageSize*(paging.Page-1)),
		sql.Named("limit", paging.PageSize),
	)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	return mdb.processRows(tx, rows, revComp, &result)
}

// More complex boolean search
func (mdb *MotifDB) BoolSearch(q string,
	datasets []string,
	paging *Paging,
	revComp bool) (*MotifSearchResult, error) {

	// clamp page number
	paging.Page = max(paging.Page, 1) //sys.Clamp(page.Page, 1, 1000)

	// clamp page size
	paging.PageSize = sys.Clamp(paging.PageSize, MinPageSize, MaxPageSize)

	// key := fmt.Sprintf("q:%s:d:%s:p:%d:ps:%d:rev:%t:mode:bool",
	// 	q,
	// 	strings.Join(datasets, ","),
	// 	page.Page,
	// 	page.PageSize,
	// 	revComp)

	// if cached, found := mdb.cache.Get(key); found {
	// 	log.Debug().Msgf("motif cache hit for key %s", key)
	// 	return cached.(*MotifSearchResult), nil
	// }

	result := MotifSearchResult{Total: 0,
		Paging: paging,
		Motifs: make([]*Motif, 0, 20)}

	// rows, err := mdb.db.Query(SearchSql,
	// 	sql.Named("id", search),
	// 	sql.Named("q", fmt.Sprintf("%%%s%%", search)))

	tx, err := mdb.db.Begin()

	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	err = addTempDatasets(tx, datasets)

	if err != nil {
		return nil, err
	}

	tree, err := query.SqlBoolTree(q)

	if err != nil {
		return nil, err
	}

	motifIdWhere, err := query.SqlBoolQueryFromTree(tree, func(placeholderIndex int, value string, addParens bool) string {
		// for slqlite
		ph := query.IndexedParam(placeholderIndex)

		// if not {
		// 	return "(m.id NOT LIKE " + ph + " AND m.motif_id NOT LIKE " + ph + " AND m.motif_name NOT LIKE " + ph + ")"
		// }

		// we use like even for exact matches to allow for case insensitivity
		return query.AddParens("m.public_id = "+ph+" OR m.motif_id LIKE "+ph+" OR m.motif_name LIKE "+ph, addParens)

	})

	if err != nil {
		return nil, err
	}

	datasetIdWhere, err := query.SqlBoolQueryFromTree(tree, func(placeholderIndex int, value string, addParens bool) string {
		// for slqlite
		ph := query.IndexedParam(placeholderIndex)
		// if not {
		// 	return "(d.id NOT LIKE " + ph + " AND d.name NOT LIKE " + ph + ")"
		// }

		return query.AddParens("d.public_id = "+ph+" OR d.name LIKE "+ph, addParens)
	})

	if err != nil {
		return nil, err
	}

	motifIdSql := motifIdWhere.Sql
	datasetIdSql := datasetIdWhere.Sql

	args := []any{sql.Named("limit", paging.PageSize),
		sql.Named("offset", paging.PageSize*(paging.Page-1))}

	args = append(args, query.IndexedNamedArgs(motifIdWhere.Args)...)

	// append query args as named parameters to match

	// countSql := fmt.Sprintf(BoolCountSql,
	// 	motifIdSql,
	// 	datasetIdSql)

	query := strings.Replace(BoolCountSql, "<<MOTIFS>>", motifIdSql, 1)
	query = strings.Replace(query, "<<DATASETS>>", datasetIdSql, 1)

	//log.Debug().Msgf("count sql: %s", countSql)
	//log.Debug().Msgf("count args: %v", args)

	rows, err := tx.Query(query, args...)

	// records in total

	if err != nil {
		log.Debug().Msgf("bool search count error: %s", err)
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var dataset Dataset

		err := rows.Scan(&dataset.PublicId, &dataset.Name, &dataset.MotifCount)

		if err != nil {
			return nil, err
		}

		result.Total += dataset.MotifCount
	}

	// calculate total pages
	paging.Pages = (result.Total + paging.PageSize - 1) / paging.PageSize

	query = strings.Replace(BoolSearchSql, "<<MOTIFS>>", motifIdSql, 1)
	query = strings.Replace(query, "<<DATASETS>>", datasetIdSql, 1)

	//log.Debug().Msgf("search sql: %s", searchSql)

	// make dynamic args list

	rows, err = tx.Query(query, args...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	return mdb.processRows(tx, rows, revComp, &result)
}

// both search methods use this to process rows and fetch weights
func (mdb *MotifDB) processRows(
	tx *sql.Tx,
	rows *sql.Rows,
	revComp bool,
	result *MotifSearchResult) (*MotifSearchResult, error) {
	var genes string
	// we ignore dataset name here since we fetch it in the main query
	// but it is part of the query for sorting
	//var datasetName string

	for rows.Next() {
		var motif Motif
		motif.Dataset = &sys.Entity{}

		err := rows.Scan(&motif.Dataset.PublicId,
			&motif.Dataset.Name,
			&motif.PublicId,
			&motif.MotifId,
			&motif.Name,
			&genes)

		log.Debug().Msgf("processing motif: %v", motif)

		if err != nil {
			return nil, err
		}

		motif.Genes = strings.Split(genes, "|")

		motif.Weights = make([][]float64, 0, 20)

		weightRows, err := tx.Query(WeightsSql,
			sql.Named("id", motif.PublicId))

		if err != nil {
			return nil, err
		}

		defer weightRows.Close()

		var a, c, g, t float64

		for weightRows.Next() {
			err := weightRows.Scan(&a, &c, &g, &t)

			if err != nil {
				return nil, err
			}

			motif.Weights = append(motif.Weights, []float64{a, c, g, t})
		}

		// weight are stored as a string of floats in database
		// which we can parse as json
		//json.Unmarshal([]byte(weights), &motif.Weights)

		// reverse position order
		if revComp {
			// reverse order of weights
			slices.Reverse(motif.Weights)

			// reverse order of values in each position
			// to complement so A becomes T and C becomes G
			for _, pw := range motif.Weights {
				slices.Reverse(pw)
			}
		}

		result.Motifs = append(result.Motifs, &motif)
	}

	// if useCache {
	// 	mdb.cache.Add(key, result)
	// }

	return result, nil

}

func addTempDatasets(tx *sql.Tx, datasets []string) error {
	// make temp table and insert datasets
	_, err := tx.Exec(TempDatasetTableSql)

	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(InsertTempDatasetSql)

	if err != nil {
		return err
	}

	defer stmt.Close()

	for _, dataset := range datasets {
		_, err := stmt.Exec(sql.Named("id", dataset))

		if err != nil {
			return err
		}
	}

	return nil
}

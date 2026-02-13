package motifs

import (
	"database/sql"
	"fmt"
	"time"

	"slices"
	"strings"

	"github.com/antonybholmes/go-sys"
	"github.com/antonybholmes/go-sys/log"
	"github.com/antonybholmes/go-sys/query"
	"github.com/hashicorp/golang-lru/v2/expirable"
)

type (
	Paging struct {
		Page     int `json:"page"`
		PageSize int `json:"pageSize"`
	}

	DatasetCount struct {
		PublicId   string `json:"id"`
		MotifCount int    `json:"motifCount"`
	}

	Dataset struct {
		DatasetCount
		Name string `json:"name"`
	}

	Motif struct {
		PublicId  string      `json:"id"`
		Dataset   string      `json:"dataset"`
		MotifId   string      `json:"motifId"`
		MotifName string      `json:"motifName"`
		Genes     []string    `json:"genes"`
		Weights   [][]float32 `json:"weights"`
	}

	MotifToGeneMap map[string]Motif

	MotifDB struct {
		db    *sql.DB
		cache *expirable.LRU[string, any]
		file  string
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
		m.dataset_id, 
		COUNT(m.id) AS total 
		FROM (
			-- Direct match on motifs.id
			SELECT 
			d.public_id AS dataset_id, 
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
			d.public_id AS dataset_id, 
			m.id
			FROM motifs m
			JOIN datasets d ON m.dataset_id = d.id
			JOIN temp_datasets td ON d.public_id = td.id
			JOIN temp_queries tp ON 
				d.public_id = tp.id OR 
				d.name LIKE tp.query
		) AS m
		GROUP BY m.dataset_id;`

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
		m.motif_public_id, 
		m.dataset_public_id,
		m.motif_id, 
		m.motif_name, 
		m.genes 
		FROM (
			-- Direct match on motifs.id
			SELECT 
			m.public_id AS motif_public_id, 
			d.public_id AS dataset_public_id,
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
			m.public_id AS motif_public_id, 
			d.public_id AS dataset_public_id,
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
			m.motif_public_id
		LIMIT :limit 
		OFFSET :offset;`

	BoolCountSql = `SELECT
		m.dataset_id,
		COUNT(m.id) AS total 
		FROM (
			-- Direct match on motifs.id
			SELECT 
			d.public_id AS dataset_id,
			m.id
			FROM motifs m
			JOIN datasets d ON m.dataset_id = d.id
			JOIN temp_datasets td ON d.public_id = td.id
			WHERE <<MOTIFS>>

			UNION

			-- search datasets
			SELECT 
			d.public_id AS dataset_id,
			m.id
			FROM motifs m
			JOIN datasets d ON m.dataset_id = d.id
			JOIN temp_datasets td ON d.public_id = td.id
			WHERE <<DATASETS>>
		) AS m
		GROUP BY m.dataset_id;`

	BoolSearchSql = `SELECT 
		m.motif_public_id,
		m.dataset_public_id,
		m.motif_id, 
		m.motif_name, 
		m.genes 
		FROM (
			-- Direct match on motifs.id
			SELECT 
			m.public_id AS motif_public_id,
			d.public_id AS dataset_public_id,
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
			m.public_id motif_public_id, 
			d.public_id AS dataset_public_id,
			m.motif_id, 
			m.motif_name, 
			m.genes
			FROM motifs m
			JOIN datasets d ON m.dataset_id = d.id
			JOIN temp_datasets td ON d.public_id = td.id
			WHERE <<DATASETS>>
		) AS m
		ORDER BY m.dataset_id, m.motif_id ASC 
		LIMIT :limit 
		OFFSET :offset;`

	WeightsSql = `SELECT 
		w.position, 
		w.a, 
		w.c, 
		w.g, 
		w.t 
		FROM weights w
		JOIN motifs m ON w.motif_id = m.id
		WHERE m.public_id = :id 
		ORDER BY w.position`
)

func NewMotifDB(file string) *MotifDB {
	return &MotifDB{file: file,
		cache: expirable.NewLRU[string, any](CacheSize, nil, CacheExpiry),
		db:    sys.Must(sql.Open(sys.Sqlite3DB, file))}
}

func (mdb *MotifDB) Datasets(useCache bool) ([]*Dataset, error) {

	if cached, found := mdb.cache.Get("datasets"); found {
		log.Debug().Msgf("motif cache hit for datasets")
		return cached.([]*Dataset), nil
	}

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

	if useCache {
		mdb.cache.Add("datasets", datasets)
	}

	return datasets, nil
}

func (mdb *MotifDB) Search(queries []string,
	datasets []string,
	page *Paging,
	revComp bool,
	useCache bool) (*MotifSearchResult, error) {
	// clamp page number
	page.Page = max(page.Page, 1)

	// clamp page size
	page.PageSize = min(max(page.PageSize, MinPageSize), MaxPageSize)

	key := fmt.Sprintf("q:%s:d:%s:p:%d:ps:%d:rev:%t",
		strings.Join(queries, ","),
		strings.Join(datasets, ","),
		page.Page,
		page.PageSize,
		revComp)

	if cached, found := mdb.cache.Get(key); found {
		log.Debug().Msgf("motif cache hit for key %s", key)
		return cached.(*MotifSearchResult), nil
	}

	result := MotifSearchResult{Total: 0,
		Paging: page,

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

	//_, err = tx.Exec(DropTempPatternSql)

	//if err != nil {
	//	return nil, err
	//}

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
		var dataset DatasetCount

		err := rows.Scan(&dataset.PublicId, &dataset.MotifCount)

		if err != nil {
			return nil, err
		}

		result.Total += dataset.MotifCount
	}

	log.Debug().Msgf("total motifs found: %d", result.Total)

	// rows, err := tx.Query(SearchSql,
	// 	sql.Named("id", search),
	// 	sql.Named("q", q),
	// 	sql.Named("offset", pageSize*(page-1)),
	// 	sql.Named("limit", pageSize),
	// )

	rows, err = tx.Query(SearchSql,
		sql.Named("offset", page.PageSize*(page.Page-1)),
		sql.Named("limit", page.PageSize),
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
	page *Paging,
	revComp bool,
	useCache bool) (*MotifSearchResult, error) {

	// clamp page number
	page.Page = max(page.Page, 1)

	// clamp page size
	page.PageSize = min(max(page.PageSize, MinPageSize), MaxPageSize)

	key := fmt.Sprintf("q:%s:d:%s:p:%d:ps:%d:rev:%t:mode:bool",
		q,
		strings.Join(datasets, ","),
		page.Page,
		page.PageSize,
		revComp)

	if cached, found := mdb.cache.Get(key); found {
		log.Debug().Msgf("motif cache hit for key %s", key)
		return cached.(*MotifSearchResult), nil
	}

	result := MotifSearchResult{Total: 0,
		Paging: page,
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

	args := []any{sql.Named("limit", page.PageSize),
		sql.Named("offset", page.PageSize*(page.Page-1))}

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
		var dataset DatasetCount

		err := rows.Scan(&dataset.PublicId, &dataset.MotifCount)

		if err != nil {
			return nil, err
		}

		result.Total += dataset.MotifCount
	}

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

		err := rows.Scan(&motif.PublicId,
			&motif.Dataset,
			//&datasetName,
			&motif.MotifId,
			&motif.MotifName,
			&genes)

		log.Debug().Msgf("processing motif: %v", motif)

		if err != nil {
			return nil, err
		}

		motif.Genes = strings.Split(genes, "|")

		motif.Weights = make([][]float32, 0, 20)

		weightRows, err := tx.Query(WeightsSql,
			sql.Named("id", motif.PublicId))

		if err != nil {
			log.Debug().Msgf("error querying weights for motif %s: %s", motif.PublicId, err)
			return nil, err
		}

		defer weightRows.Close()

		var position int
		var a, c, g, t float32

		for weightRows.Next() {
			log.Debug().Msgf("scanning weight row for motif %s", motif.PublicId)
			err := weightRows.Scan(&position, &a, &c, &g, &t)

			if err != nil {
				log.Debug().Msgf("error scanning weight row for motif %s: %s", motif.PublicId, err)
				return nil, err
			}

			motif.Weights = append(motif.Weights, []float32{a, c, g, t})
		}

		log.Debug().Msgf("scanning weight row for motif values: %v", motif.Weights)

		// weight are stored as a string of floats in database
		// which we can parse as json
		//json.Unmarshal([]byte(weights), &motif.Weights)

		// reverse position order
		if revComp {
			slices.Reverse(motif.Weights)

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

	for _, ds := range datasets {
		_, err := stmt.Exec(sql.Named("id", ds))

		if err != nil {
			return err
		}
	}

	return nil
}

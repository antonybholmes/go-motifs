package routes

import (
	"errors"
	"strconv"
	"strings"

	"github.com/antonybholmes/go-motifs"
	"github.com/antonybholmes/go-motifs/motifsdb"
	"github.com/antonybholmes/go-sys/log"
	"github.com/antonybholmes/go-sys/query"
	"github.com/antonybholmes/go-web"
	"github.com/gin-gonic/gin"
)

var (
	ErrSearchTooShort = errors.New("search too short")
)

type (
	ReqParams struct {
		Search     string `json:"search"`
		Exact      bool   `json:"exact"`
		Reverse    bool   `json:"reverse"`
		Complement bool   `json:"complement"`
	}

	MotifRes struct {
		Search     string          `json:"search"`
		Motifs     []*motifs.Motif `json:"motifs"`
		Reverse    bool            `json:"reverse"`
		Complement bool            `json:"complement"`
	}
)

func ParseParamsFromPost(c *gin.Context) (*ReqParams, error) {

	var params ReqParams

	err := c.Bind(&params)

	if err != nil {
		return nil, err
	}

	return &params, nil
}

func DatasetsRoute(c *gin.Context) {

	// Don't care about the errors, just plug empty list into failures
	datasets, err := motifsdb.Datasets()

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", datasets)

	//web.MakeDataResp(c, "", mutationdbcache.GetInstance().List())
}

func SearchRoute(c *gin.Context) {

	// params, err := ParseParamsFromPost(c)

	// if err != nil {
	// 	c.Error(err)
	// 	return
	// }

	q := c.Query("q")

	if len(q) < motifs.MinSearchLen {
		web.BadReqResp(c, ErrSearchTooShort)
		return
	}

	q = query.SanitizeQuery(q)

	queries := strings.Split(q, ",")

	// trim spaces around each query
	queriesTrimmed := make([]string, 0, len(queries))

	for _, query := range queries {
		queriesTrimmed = append(queriesTrimmed, strings.TrimSpace(query))
	}

	page, err := strconv.Atoi(c.Query("page"))

	if err != nil || page < 1 {
		page = 1
	}

	pageSize, err := strconv.Atoi(c.Query("pageSize"))

	if err != nil {
		pageSize = motifs.MinPageSize
	}

	searchMode := c.Query("searchMode")

	var result *motifs.MotifSearchResult

	// we can enable bool search mode for more complex queries
	if strings.HasPrefix(searchMode, "b") {
		log.Debug().Msgf("bool search mode")

		result, err = motifsdb.BoolSearch(q, page, pageSize, false, false)
	} else {
		// regular search mode
		log.Debug().Msgf("queries: %v", queriesTrimmed)

		result, err = motifsdb.Search(queriesTrimmed, page, pageSize, false, false)
	}

	if err != nil {
		log.Debug().Msgf("motif %s", err)
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "",
		result)

	//web.MakeDataResp(c, "", mutationdbcache.GetInstance().List())
}

package routes

import (
	"errors"
	"strings"

	"github.com/antonybholmes/go-motifs"
	"github.com/antonybholmes/go-motifs/motifsdb"
	"github.com/antonybholmes/go-sys/log"
	"github.com/antonybholmes/go-sys/query"
	"github.com/antonybholmes/go-web"
	"github.com/gin-gonic/gin"
)

type (
	ReqParams struct {
		Q string `json:"q" form:"q"`
		//Exact      bool     `json:"exact"`
		RevComp    bool     `json:"revComp"`
		Datasets   []string `json:"datasets"`
		Page       int      `json:"page" form:"page"`
		PageSize   int      `json:"pageSize" form:"pageSize"`
		SearchMode string   `json:"searchMode" form:"searchMode"`
		UseCache   string   `json:"cache" form:"cache"`
	}

	MotifRes struct {
		Search     string          `json:"search"`
		Motifs     []*motifs.Motif `json:"motifs"`
		Reverse    bool            `json:"reverse"`
		Complement bool            `json:"complement"`
	}
)

var (
	ErrSearchTooShort = errors.New("search too short")
)

// utility to convert cache param string to bool
// default is true if empty
func useCacheFromString(s string) bool {
	if s == "" {
		return true
	}

	sLower := strings.ToLower(s)

	if sLower == "1" || strings.HasPrefix(sLower, "t") || strings.HasPrefix(sLower, "y") {
		return true
	}

	return false
}

func ParseParamsFromPost(c *gin.Context) (*ReqParams, error) {

	var params ReqParams

	err := web.BindQueryAndJSON(c, &params)

	if err != nil {
		return nil, err
	}

	return &params, nil
}

func DatasetsRoute(c *gin.Context) {
	params, err := ParseParamsFromPost(c)

	if err != nil {
		c.Error(err)
		return
	}

	useCache := useCacheFromString(params.UseCache)

	// Don't care about the errors, just plug empty list into failures
	datasets, err := motifsdb.Datasets(useCache)

	if err != nil {
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "", datasets)

	//web.MakeDataResp(c, "", mutationdbcache.GetInstance().List())
}

func SearchRoute(c *gin.Context) {

	params, err := ParseParamsFromPost(c)

	if err != nil {
		c.Error(err)
		return
	}

	//log.Debug().Msgf("motif search %v", params)

	q := params.Q

	if len(q) < motifs.MinSearchLen {
		web.BadReqResp(c, ErrSearchTooShort)
		return
	}

	q = query.SanitizeQuery(q)

	// // which datasets to search in
	// datasets := sys.NewStringSet()

	// for _, ds := range params.Datasets {
	// 	datasets.Add(ds)
	// }

	useCache := useCacheFromString(params.UseCache)

	var result *motifs.MotifSearchResult

	page := motifs.Paging{
		Page:     max(params.Page, 1),
		PageSize: max(params.PageSize, motifs.MinPageSize),
	}

	// we can enable bool search mode for more complex queries
	if strings.HasPrefix(params.SearchMode, "b") {
		log.Debug().Msgf("bool search mode")

		result, err = motifsdb.BoolSearch(q, params.Datasets, &page, false, useCache)
	} else {
		queries := strings.Split(q, ",")

		// trim spaces around each query
		queriesTrimmed := make([]string, 0, len(queries))

		for _, query := range queries {
			queriesTrimmed = append(queriesTrimmed, strings.TrimSpace(query))
		}

		result, err = motifsdb.Search(queriesTrimmed, params.Datasets, &page, false, useCache)
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

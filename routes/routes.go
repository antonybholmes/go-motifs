package routes

import (
	"errors"
	"strconv"

	"github.com/antonybholmes/go-motifs"
	"github.com/antonybholmes/go-motifs/motifsdb"
	"github.com/antonybholmes/go-sys/log"
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

	search := c.Query("q")

	if len(search) < motifs.MinSearchLen {
		web.BadReqResp(c, ErrSearchTooShort)
		return
	}

	page, err := strconv.Atoi(c.Query("page"))

	if err != nil || page < 1 {
		page = 1
	}

	pageSize, err := strconv.Atoi(c.Query("pageSize"))

	if err != nil {
		pageSize = motifs.MinPageSize
	}

	//log.Debug().Msgf("motif %v", params)

	// Don't care about the errors, just plug empty list into failures
	result, err := motifsdb.Search(search, page, pageSize, false, false)

	if err != nil {
		log.Debug().Msgf("motif %s", err)
		c.Error(err)
		return
	}

	web.MakeDataResp(c, "",
		result)

	//web.MakeDataResp(c, "", mutationdbcache.GetInstance().List())
}

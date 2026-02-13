package motifs

import (
	"fmt"
	"testing"
)

func TestGenes(t *testing.T) {

	db := NewMotifDB("../data/modules/motifs/motifs.db")

	page := Paging{
		Page:     1,
		PageSize: 100,
	}

	res, err := db.Search([]string{"ADNP_IRX_SIX_ZHX.p2"}, []string{}, &page, false, false)

	if err != nil {
		fmt.Printf("%s", err)
	}

	for _, motif := range res.Motifs {
		fmt.Printf("%v %v %v", motif.Dataset, motif.Genes, motif.Weights)
	}

}

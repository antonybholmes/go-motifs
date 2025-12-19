package motifs

import (
	"fmt"
	"testing"
)

func TestGenes(t *testing.T) {

	db := NewMotifDB("../data/modules/motifs/motifs.db")

	res, err := db.Search([]string{"ADNP_IRX_SIX_ZHX.p2"}, 1, 100, false, false)

	if err != nil {
		fmt.Printf("%s", err)
	}

	for _, motif := range res.Motifs {
		fmt.Printf("%v %v %v", motif.Dataset, motif.Genes, motif.Weights)
	}

}

package motifs

import (
	"fmt"
	"testing"
)

func TestGenes(t *testing.T) {

	db := NewMotifDB("../data/modules/motifs/motifs.db")

	motifs, err := db.Search("ADNP_IRX_SIX_ZHX.p2")

	if err != nil {
		fmt.Printf("%s", err)
	}

	for _, motif := range motifs {
		fmt.Printf("%v %v %v", motif.Dataset, motif.Genes, motif.Weights)
	}

}

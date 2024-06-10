package motiftogene

import (
	"fmt"
	"testing"
)

func TestGenes(t *testing.T) {

	db := NewMotifToGeneDB("data/modules/motiftogene/motiftogene.json")

	gene, err := db.Convert("ADNP_IRX_SIX_ZHX.p2")

	if err != nil {
		fmt.Printf("%s", err)
	}

	fmt.Printf("%v %v", gene.Sources, gene.Genes)
}

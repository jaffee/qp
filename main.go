package main

import (
	"fmt"
	"log"

	"github.com/jaffee/commandeer"
	"github.com/pilosa/go-pilosa"
	"github.com/pkg/errors"
)

type Main struct {
	PilosaHosts []string
}

func NewMain() *Main {
	return &Main{
		PilosaHosts: []string{"localhost:10101"},
	}
}

func (m *Main) Run() error {
	client, err := pilosa.NewClient(m.PilosaHosts)
	if err != nil {
		return errors.Wrap(err, "getting client")
	}

	schema, err := client.Schema()
	idx := schema.Index("dcontracts")
	typeF := idx.Field("type")
	prodF := idx.Field("prod")
	startF := idx.Field("start")
	lastF := idx.Field("last")

	fmt.Println("week | type             | prod             |     count")
	for i := 0; i < 365*3; i += 7 {
		query := idx.GroupByFilter(idx.Intersect(startF.LT(i+7), lastF.GTE(i)), typeF.Rows(), prodF.Rows())
		resp, err := client.Query(query)
		if err != nil {
			return errors.Wrap(err, "querying")
		}

		res := resp.Result().GroupCounts()
		for _, gc := range res {
			fmt.Printf("%5d %20s %20s %12d\n", i/7, gc.Groups[0].RowKey, gc.Groups[1].RowKey, gc.Count)
		}
	}

	return nil
}

func main() {
	err := commandeer.Run(NewMain())
	if err != nil {
		log.Fatal(err)
	}
}

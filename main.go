package main

import (
	"context"
	"fmt"
	"log"

	"github.com/blugelabs/bluge"
	zerolog "github.com/rs/zerolog/log"
	"github.prabhatsharma.com/bluges3/directory"
)

func main() {

	// cfg := bluge.DefaultConfig("./data/")
	cfg := directory.GetS3Config()
	writer, err := bluge.OpenWriter(cfg)
	if err != nil {
		log.Fatal(err)
	}

	doc1 := bluge.NewDocument("1").
		AddField(bluge.NewTextField("name", "Prabhat Sharma").StoreValue())

	writer.Update(doc1.ID(), doc1)

	doc2 := bluge.NewDocument("2").
		AddField(bluge.NewTextField("name", "Alloy").StoreValue())

	err = writer.Update(doc2.ID(), doc2)
	if err != nil {
		log.Fatal("error updating document: ", err)
	}

	zerolog.Print("document indexed")

	reader, err := writer.Reader()
	if err != nil {
		zerolog.Print("error accessing reader: %v", err)
	}

	query := bluge.NewFuzzyQuery("alloy").SetField("name")

	searchRequest := bluge.NewTopNSearch(10, query)

	dmi, err := reader.Search(context.Background(), searchRequest)
	if err != nil {
		zerolog.Print("error searching: %v", err)
	}

	next, err := dmi.Next()
	for err == nil && next != nil {
		err = next.VisitStoredFields(func(field string, value []byte) bool {
			if field == "_id" {
				id := string(value)
				fmt.Println(id)
				return true
			}
			return true
		})
		if err != nil {
			zerolog.Print("error accessing stored fields: %v", err)
		}

		next, _ = dmi.Next()
	}

	// writer.Close()

}

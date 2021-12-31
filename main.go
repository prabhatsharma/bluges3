package main

import (
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
	defer func() {
		cerr := writer.Close()
		if cerr != nil {
			log.Fatal(cerr)
		}
	}()

	doc := bluge.NewDocument("a").
		AddField(bluge.NewTextField("name", "marty"))

	err = writer.Update(doc.ID(), doc)
	if err != nil {
		log.Fatal(err)
	}

	zerolog.Print("document indexed")
}

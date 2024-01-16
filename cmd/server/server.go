package main

import (
	"github.com/alikonhz/sqlitekv/api"
	"github.com/alikonhz/sqlitekv/kvstore"
)

func main() {
	store, err := kvstore.NewKvStore("server.sqlite")
	if err != nil {
		panic(err)
	}

	srv := api.NewHttpApiServer(store)
	err = srv.Start(":5050")
	if err != nil {
		panic(err)
	}
}

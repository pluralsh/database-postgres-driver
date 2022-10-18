package main

import (
	_ "github.com/lib/pq"
	"github.com/pluralsh/database-postgres-driver/pkg/postgres"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "postgres"
)

func main() {

	pgDb := postgres.Postgres{
		User:     user,
		Password: password,
		Host:     host,
		Port:     port,
	}

	err := pgDb.DeleteDatabase("%s")
	CheckError(err)
}

func CheckError(err error) {
	if err != nil {
		panic(err)
	}
}

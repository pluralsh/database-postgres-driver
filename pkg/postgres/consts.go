package postgres

const (
	GetDatabasesSQL   = `SELECT datname FROM pg_database;`
	CreateDatabaseSQL = `CREATE DATABASE "%s";`
	DeleteDatabaseSQL = `DROP DATABASE "%s";`
)

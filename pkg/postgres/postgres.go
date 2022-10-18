package postgres

import (
	"database/sql"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

type Postgres struct {
	User     string
	Password string
	Host     string
	Port     int
}

func (c *Postgres) ConnectionString(dbname string) string {
	if dbname == "" {
		dbname = "postgres"
	}

	return fmt.Sprintf("host='%s' port=%d dbname='%s' sslmode=require user='%s' password='%s'",
		c.Host,
		c.Port,
		dbname,
		c.User,
		c.Password)
}

func (c *Postgres) getDatabases() (dbs []string, err error) {

	var rows *sql.Rows
	db, err := sql.Open("postgres", c.ConnectionString(""))
	if err != nil {
		return nil, err
	}
	defer func(db *sql.DB) {
		if err := db.Close(); err != nil {
			klog.Errorf("Failed to close DB connection")
		}
	}(db)

	if rows, err = db.Query(GetDatabasesSQL); err != nil {
		return nil, fmt.Errorf("could not query database: %v", err)
	}
	defer func() {
		if err2 := rows.Close(); err2 != nil {
			if err != nil {
				err = fmt.Errorf("error when closing query cursor: %v, previous error: %v", err2, err)
			} else {
				err = fmt.Errorf("error when closing query cursor: %v", err2)
			}
			klog.Error(err)
		}
	}()

	dbs = []string{}

	for rows.Next() {
		var dbname string

		if err = rows.Scan(&dbname); err != nil {
			return nil, fmt.Errorf("error when processing row: %v", err)
		}
		dbs = append(dbs, dbname)
	}

	return dbs, err
}

func (c *Postgres) CreateDatabase(dbName string) (err error) {
	databases, err := c.getDatabases()
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("Can't get databases %v", err))
	}
	for _, existingDBName := range databases {
		if dbName == existingDBName {
			return status.Error(codes.AlreadyExists, "Database already exists")
		}
	}
	db, err := sql.Open("postgres", c.ConnectionString(""))
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("Can't connect %v", err))
	}
	defer func(db *sql.DB) {
		if err := db.Close(); err != nil {
			klog.Errorf("Failed to close DB connection")
		}
	}(db)
	if _, err := db.Exec(fmt.Sprintf(CreateDatabaseSQL, dbName)); err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("Can't create %v", err))
	}
	return nil
}

func (c *Postgres) DeleteDatabase(dbName string) (err error) {
	databases, err := c.getDatabases()
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("Can't get databases %v", err))
	}
	for _, existingDBName := range databases {
		if dbName == existingDBName {
			db, err := sql.Open("postgres", c.ConnectionString(""))
			if err != nil {
				return status.Error(codes.Internal, fmt.Sprintf("Can't connect %v", err))
			}
			defer func(db *sql.DB) {
				if err := db.Close(); err != nil {
					klog.Errorf("Failed to close DB connection")
				}
			}(db)
			if _, err := db.Exec(fmt.Sprintf(DeleteDatabaseSQL, dbName)); err != nil {
				return status.Error(codes.Internal, fmt.Sprintf("Can't delete database %v", err))
			}
			return nil
		}
	}

	return status.Error(codes.NotFound, "Database not found")
}

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	pgx "github.com/jackc/pgx/v5"
)

type Postgres struct {
	Host               string
	Port               string
	User               string
	Password           string
	DataBaseName       string
	RecordingProcedure string
	Conn               *pgx.Conn
	isReadyConn        bool
}

func (pg *Postgres) pgEnv(host, port, user, password, dbName string) {
	pg.Host = getEnvStr(host, "localhost")
	pg.Port = getEnvStr(port, "5432")
	pg.User = getEnvStr(user, "")
	pg.Password = getEnvStr(password, "")
	pg.DataBaseName = getEnvStr(dbName, "postgres")
	pg.RecordingProcedure = getEnvStr("SERVICE_PG_PROCEDURE", "call device.check_section($1, $2)")
	pg.isReadyConn = false
}

func getEnvStr(key, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if value, exists := os.LookupEnv(key); exists {
		val, err := strconv.Atoi(value)
		if err != nil {
			log.Fatal(err)
		}
		return val
	}
	return defaultVal
}

func (pg *Postgres) connPg() {
	dbURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", pg.User, pg.Password, pg.Host, pg.Port, pg.DataBaseName)
	var err error
	pg.Conn, err = pgx.Connect(context.Background(), dbURL)
	if err != nil {
		log.Printf("QueryRow failed: %v\n", err)
	} else {
		pg.isReadyConn = true
		log.Printf("Connect DB is ready")
	}
}

func (pg *Postgres) requestDb(msg []byte, offset_msg int64) error {
	_, err := pg.Conn.Exec(context.Background(), pg.RecordingProcedure, msg, offset_msg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		return err
	}
	return nil
}

func (pg *Postgres) getOffset() int {
	var offset_msg int
	if pg.isReadyConn {
		err := pg.Conn.QueryRow(context.Background(), "SELECT device.get_offset();").Scan(&offset_msg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		}
		return offset_msg
	}
	return 0
}

func (pg *Postgres) connPgloop() {
	for {
		if !pg.isReadyConn {
			pg.connPg()
		} else {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

}

func main() {
	configPG1 := Postgres{}
	configPG1.pgEnv("HOST_DB_1", "PORT_DB_1", "USERNAME_DB_1", "PASSWORD_DB_1", "DBNAME_DB_1")
	configPG1.connPgloop()

	configPG2 := Postgres{}
	configPG2.pgEnv("HOST_DB_2", "PORT_DB_2", "USERNAME_DB_2", "PASSWORD_DB_2", "DBNAME_DB_2")
	configPG2.connPgloop()

}

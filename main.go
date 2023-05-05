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
	chunk              int
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
		log.Println("Connect DB is ready:", pg.DataBaseName)
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

func (pg *Postgres) getPartition() []string {
	var rowVal string
	var partition []string
	rows, err := pg.Conn.Query(context.Background(), `SELECT child.relname AS child
		FROM pg_inherits
			JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
			JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
			JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
			JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
		WHERE parent.relname='messages';`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
	}

	for rows.Next() {
		err = rows.Scan(&rowVal)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed scan: %v\n", err)
		}
		partition = append(partition, rowVal)
	}
	return partition
}

func (pg *Postgres) getCountRows(query string) int {
	var rowsCount int
	if pg.isReadyConn {
		err := pg.Conn.QueryRow(context.Background(), query).Scan(&rowsCount)
		if err != nil {
			fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		}
		return rowsCount
	}
	return 0
}

func (pg *Postgres) getOffset() int {
	var rowsCount int
	if pg.isReadyConn {
		err := pg.Conn.QueryRow(context.Background(), "SELECT offset_msg FROM device.messages ORDER BY offset_msg DESC LIMIT 1;").Scan(&rowsCount)
		if err != nil {
			fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		}
		return rowsCount
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

func (pg *Postgres) reader(offset int, msgChan chan map[string]interface{}) {
	var (
		rowVal map[string]interface{}
	)

	for {
		rows, err := pg.Conn.Query(context.Background(),
			`
			SELECT row_to_json(d.*) FROM (
			SELECT 
				dda.id,
				dda.received_time as created_at,
				created_id,
				device_id,
				object_id,
				mes_id,
				mes_time,
				co.code as mes_code,
				(dda."data" -> 'status_info') mes_status,
				dda."data" as mes_data,
				co.const_value as event_value,
				event_data
			FROM
				sh_ilo.data_device_archive dda 
				JOIN sh_data.v_constants co on co.code = dda.event and co.class in ('DBMSGTYPE', 'DBLOGICTYPE')
			WHERE dda.id > $1::int8
			ORDER BY dda.id
			LIMIT $2::int4) d;`, offset, pg.chunk)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		}

		var countRow int
		for rows.Next() {
			// err = rows.Scan
			err = rows.Scan(&rowVal)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed scan: %v\n", err)
			}

			msgChan <- rowVal
			countRow++
		}
		offset = int(rowVal["id"].(float64))
		log.Println("Records read: ", countRow)
		time.Sleep(1 * time.Millisecond)
	}
	close(msgChan)
}

func serializeRow(row map[string]interface{}) []interface{} {
	var res []interface{}
	columns := []string{
		"id",
		"created_at",
		"created_id",
		"device_id",
		"object_id",
		"mes_id",
		"mes_time",
		"mes_code",
		"mes_status",
		"mes_data",
		"event_value",
		"event_data",
	}
	for _, value := range columns {
		if value == "created_at" || value == "mes_time" {
			timeVal, err := time.Parse("2006-01-02T15:04:05", row[value].(string))
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed time.Parse: %v\n", err)
			}
			res = append(res, timeVal)
			continue
		}
		res = append(res, row[value])
	}
	return res
}

func checkPartition(partitions []string, section string) bool {
	for _, n := range partitions {
		if section == n {
			return true
		}
	}
	return false
}

func (pg *Postgres) createSection(id int, section string) {
	query := fmt.Sprintf("CREATE TABLE %s PARTITION OF device.messages FOR VALUES IN (%d)", section, id)
	_, err := pg.Conn.Exec(context.Background(), query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query CREATE PARTITION failed: %v\n", err)
	}
	log.Println("Create section :", section)
}

func (pg *Postgres) writer(msgChan chan map[string]interface{}, partition []string) {
	var (
		rows   [][]interface{}
		serRow []interface{}
		count  int
	)

	// Получение сообщений из канала
	for msg := range msgChan {

		// Заполенение чанка
		if count < pg.chunk {
			id := int(msg["object_id"].(float64))
			section := fmt.Sprintf("message_%d", id)

			if !checkPartition(partition, section) {
				pg.createSection(id, section)
				partition = append(partition, section)
			}

			serRow = serializeRow(msg)
			rows = append(rows, serRow)
			count++
		} else {
			// Вставка в таблицу
			a := pg.getOffset()
			log.Println(a)
			log.Println(len(rows[0]))
			copyCount, err := pg.Conn.CopyFrom(
				context.Background(),
				pgx.Identifier{"device", "messages"},
				[]string{"offset_msg",
					"created_at",
					"created_id",
					"device_id",
					"object_id",
					"mes_id",
					"mes_time",
					"mes_code", "mes_status", "mes_data", "event_value", "event_data"},
				pgx.CopyFromRows(rows),
			)

			if err != nil {
				fmt.Fprintf(os.Stderr, "failed CopyFrom: %v\n", err)
			}
			log.Println("Вставлено :", copyCount)
			count = 0
			rows = nil
		}
	}
}

func main() {
	var (
		offset_msg   int
		countRowsDB1 int
		countRowsDB2 int
		partition    []string
	)

	msgChan := make(chan map[string]interface{}, 10000)
	checkChan := make(chan bool)

	configPG1 := Postgres{}
	configPG1.chunk = 10000
	configPG1.pgEnv("HOST_DB_1", "PORT_DB_1", "USERNAME_DB_1", "PASSWORD_DB_1", "DBNAME_DB_1")
	configPG1.connPgloop()

	countRowsDB1 = configPG1.getCountRows("select COUNT(id) from sh_ilo.data_device_archive")
	log.Println(countRowsDB1)

	configPG2 := Postgres{}
	configPG2.chunk = 2000
	configPG2.pgEnv("HOST_DB_2", "PORT_DB_2", "USERNAME_DB_2", "PASSWORD_DB_2", "DBNAME_DB_2")
	configPG2.connPgloop()

	countRowsDB2 = configPG2.getCountRows("select COUNT(id) from device.messages;")

	if countRowsDB2 > 0 {
		// Получение оффсета из БД2
		offset_msg = configPG2.getOffset()
		log.Println("Get offset:", offset_msg)

		partition = configPG2.getPartition()
		log.Println(partition)
	}

	go configPG1.reader(offset_msg, msgChan)

	go configPG2.writer(msgChan, partition)

	<-checkChan
}

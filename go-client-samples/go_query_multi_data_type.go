package main

import (
	"context"
	"log"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Username: "xxx",
			Password: "xxx",
		},
	})
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	query := "SELECT 1 AS test_int, 'Snowflake Version' AS test_string, '2024-12-13' AS test_date"
	log.Printf("Executing query: %s", query)

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var testInt *int64   // Use *int64 for Int64 values
	var testString string
	var testDate string

	for rows.Next() {
		if err := rows.Scan(&testInt, &testString, &testDate); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		log.Printf("Result: test_int=%d, test_string=%s, test_date=%s", *testInt, testString, testDate)
	}
}

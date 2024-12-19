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
			Username: "xx",
			Password: "xx",
		},
	})
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Query current_version()
	versionQuery := "SELECT current_version()"
	log.Printf("Executing query: %s", versionQuery)
	executeQuery(conn, versionQuery)

	// Query current_username()
	usernameQuery := "SELECT current_user()"
	log.Printf("Executing query: %s", usernameQuery)
	executeQuery(conn, usernameQuery)
}

func executeQuery(conn clickhouse.Conn, query string) {
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		log.Printf("Result: %s", result)
	}
}

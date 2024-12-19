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

	insertQuery := "INSERT INTO todo (id, created_by, title, description, status) VALUES (?, ?, ?, ?, ?)"
	log.Printf("Executing insert query: %s", insertQuery)

	// Execute the insert query
	err = conn.Exec(context.Background(), insertQuery, 99999, 54234, "test3", "clickhouse", true)
	if err != nil {
		log.Fatalf("Insert query failed: %v", err)
	}
	log.Println("Insert query executed successfully.")
}

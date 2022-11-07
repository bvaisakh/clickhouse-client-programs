package main

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"time"
)

func main() {
	ctx := context.Background()

	connection1, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{"127.0.0.1:9300"},
		DialTimeout: time.Duration(10) * time.Second,
	})
	if err != nil {
		return
	}

	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println("CLIENT #1: SELECT id, name FROM test.numbers LIMIT 10")
	fmt.Println("--------------------------------------------------------------------------------")
	rows1, err := connection1.Query(ctx, "SELECT id, name FROM test.numbers LIMIT 10")
	if err != nil {
		return
	}
	for rows1.Next() {
		var (
			col1 uint64
			col2 string
		)
		if err := rows1.Scan(&col1, &col2); err != nil {
			return
		}
		fmt.Printf("id=%d, name=%s\n", col1, col2)
	}
	rows1.Close()
	fmt.Println("--------------------------------------------------------------------------------")

	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println("CLIENT #2: SELECT id, name FROM test.numbers LIMIT 10")
	fmt.Println("--------------------------------------------------------------------------------")
	connection2, _ := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{"127.0.0.1:9300"},
		DialTimeout: time.Duration(10) * time.Second,
	})

	rows2, err := connection2.Query(ctx, "SELECT id, name FROM test.numbers LIMIT 10")
	if err != nil {
		return
	}
	for rows2.Next() {
		var (
			col1 uint64
			col2 string
		)
		if err := rows2.Scan(&col1, &col2); err != nil {
			return
		}
		fmt.Printf("id=%d, name=%s\n", col1, col2)
	}
	rows2.Close()
	fmt.Println("--------------------------------------------------------------------------------")

	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println("CLIENT #3: SELECT id, name FROM test.numbers LIMIT 10")
	fmt.Println("--------------------------------------------------------------------------------")
	connection3, _ := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{"127.0.0.1:9300"},
		DialTimeout: time.Duration(10) * time.Second,
	})

	rows3, err := connection3.Query(ctx, "SELECT id, name FROM test.numbers LIMIT 10")
	if err != nil {
		return
	}
	for rows3.Next() {
		var (
			col1 uint64
			col2 string
		)
		if err := rows3.Scan(&col1, &col2); err != nil {
			return
		}
		fmt.Printf("id=%d, name=%s\n", col1, col2)
	}
	rows3.Close()
	fmt.Println("--------------------------------------------------------------------------------")

	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println("CLIENT #1: INSERT INTO test.numbers VALUES (1, 'Vaisakh')")
	fmt.Println("--------------------------------------------------------------------------------")
	rows4, err := connection1.Query(ctx, "SELECT COUNT(*) FROM test.numbers")
	if err != nil {
		return
	}
	for rows4.Next() {
		var (
			col1 uint64
		)
		if err := rows4.Scan(&col1); err != nil {
			return
		}
		fmt.Printf("ROW COUNT BEFORE INSERT: %d\n", col1)
	}
	rows4.Close()

	fmt.Println("INSERTION IN PROGRESS...")
	connection1.Exec(ctx, "INSERT INTO test.numbers VALUES (1, 'Vaisakh')")

	rows5, err := connection1.Query(ctx, "SELECT COUNT(*) FROM test.numbers")
	if err != nil {
		return
	}
	for rows5.Next() {
		var (
			col1 uint64
		)
		if err := rows5.Scan(&col1); err != nil {
			return
		}
		fmt.Printf("ROW COUNT AFTER INSERT: %d\n", col1)
	}
	rows5.Close()
	fmt.Println("--------------------------------------------------------------------------------")
}

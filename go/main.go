package main

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"time"
)

func main() {
	ctx := context.Background()

	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println("CLIENT #1: SELECT id, name, mail FROM demo.contacts LIMIT 10")
	fmt.Println("--------------------------------------------------------------------------------")
	connection1, _ := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{"127.0.0.1:9300"},
		DialTimeout: time.Duration(10) * time.Second,
	})

	rows1, _ := connection1.Query(ctx, "SELECT id, name, mail FROM demo.contacts LIMIT 10")
	for rows1.Next() {
		var (
			col1 uint64
			col2 string
			col3 string
		)
		rows1.Scan(&col1, &col2, &col3)
		fmt.Printf("id=%d, name=%s, mail=%s\n", col1, col2, col3)
	}
	rows1.Close()
	fmt.Println("--------------------------------------------------------------------------------")

	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println("CLIENT #2: SELECT id, name, mail FROM demo.contacts LIMIT 10")
	fmt.Println("--------------------------------------------------------------------------------")
	connection2, _ := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{"127.0.0.1:9300"},
		DialTimeout: time.Duration(10) * time.Second,
	})

	rows2, _ := connection2.Query(ctx, "SELECT id, name, mail FROM demo.contacts LIMIT 10")
	for rows2.Next() {
		var (
			col1 uint64
			col2 string
			col3 string
		)
		rows2.Scan(&col1, &col2, &col3)
		fmt.Printf("id=%d, name=%s, mail=%s\n", col1, col2, col3)
	}
	rows2.Close()
	fmt.Println("--------------------------------------------------------------------------------")

	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println("CLIENT #1: INSERT INTO demo.contacts (id, name, mail) VALUES (4, 'Ahamed Sinan', 'ahamed.sinan@chistadata.com')")
	fmt.Println("--------------------------------------------------------------------------------")
	rows4, _ := connection1.Query(ctx, "SELECT COUNT(*) FROM demo.contacts")
	for rows4.Next() {
		var (
			col1 uint64
		)
		rows4.Scan(&col1)
		fmt.Printf("ROW COUNT BEFORE INSERT: %d\n", col1)
	}
	rows4.Close()

	fmt.Println("INSERTION IN PROGRESS...")
	connection1.Exec(ctx, "INSERT INTO demo.contacts (id, name, mail) VALUES (4, 'Ahamed Sinan', 'ahamed.sinan@chistadata.com')")

	rows5, _ := connection1.Query(ctx, "SELECT COUNT(*) FROM demo.contacts")
	for rows5.Next() {
		var (
			col1 uint64
		)
		rows5.Scan(&col1)
		fmt.Printf("ROW COUNT AFTER INSERT: %d\n", col1)
	}
	rows5.Close()
	fmt.Println("--------------------------------------------------------------------------------")

	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println("CLIENT #3: SELECT id, name, mail FROM demo.contacts LIMIT 10")
	fmt.Println("--------------------------------------------------------------------------------")
	connection3, _ := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{"127.0.0.1:9300"},
		DialTimeout: time.Duration(10) * time.Second,
	})

	rows3, _ := connection3.Query(ctx, "SELECT id, name, mail FROM demo.contacts LIMIT 10")
	for rows3.Next() {
		var (
			col1 uint64
			col2 string
			col3 string
		)
		rows3.Scan(&col1, &col2, &col3)
		fmt.Printf("id=%d, name=%s, mail=%s\n", col1, col2, col3)
	}
	rows3.Close()
	fmt.Println("--------------------------------------------------------------------------------")

}

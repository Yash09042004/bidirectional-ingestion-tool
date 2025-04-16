package main

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func connectToClickHouse(host, port, database, user, jwtToken string) (driver.Conn, error) {
	addr := fmt.Sprintf("%s:%s", host, port)
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: user,
			Password: jwtToken,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	return conn, nil
}

// Remove the main function to resolve the redeclaration error
// func main() {
// 	host := "localhost"
// 	port := "9000"
// 	database := "default"
// 	user := "default"
// 	jwtToken := "your_jwt_token_here"
//
// 	err := connectToClickHouse(host, port, database, user, jwtToken)
// 	if err != nil {
// 		fmt.Println("Error:", err)
// 	}
// }



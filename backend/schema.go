package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type TableSchema struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
}

// GetClickHouseTables fetches available tables and their columns
func GetClickHouseTables(conn driver.Conn) ([]TableSchema, error) {
	// Get list of tables
	rows, err := conn.Query(context.Background(), "SELECT name FROM system.tables WHERE database = currentDatabase()")
	if err != nil {
		log.Printf("Error querying tables: %v", err)
		return nil, fmt.Errorf("failed to query tables: %v", err)
	}
	defer rows.Close()

	var tables []TableSchema
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			log.Printf("Error scanning table name: %v", err)
			continue
		}

		// Get columns for each table
		columnRows, err := conn.Query(context.Background(), fmt.Sprintf("SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = '%s'", tableName))
		if err != nil {
			log.Printf("Error querying columns for table %s: %v", tableName, err)
			continue
		}
		defer columnRows.Close()

		var columns []string
		for columnRows.Next() {
			var columnName, columnType string
			if err := columnRows.Scan(&columnName, &columnType); err != nil {
				log.Printf("Error scanning column name: %v", err)
				continue
			}
			columns = append(columns, fmt.Sprintf("%s (%s)", columnName, columnType))
		}

		tables = append(tables, TableSchema{
			Name:    tableName,
			Columns: columns,
		})
	}

	if err := rows.Err(); err != nil {
		log.Printf("Error iterating rows: %v", err)
		return nil, fmt.Errorf("error iterating rows: %v", err)
	}

	return tables, nil
}

// GetFlatFileSchema reads the header of a CSV/flat file to determine columns
func GetFlatFileSchema(fileName, delimiter string) ([]map[string]string, error) {
	// Get the current working directory
	wd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("failed to get working directory: %v", err)
	}

	// Create the full file path
	filePath := filepath.Join(wd, fileName)
	log.Printf("Reading schema from file: %s", filePath)

	// Open the input file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Create a CSV reader
	reader := csv.NewReader(file)
	if delimiter == "" {
		delimiter = ","
	}
	reader.Comma = rune(delimiter[0])

	// Read the header
	columns, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %v", err)
	}

	// Read the first row to infer types
	row, err := reader.Read()
	if err != nil && err.Error() != "EOF" {
		return nil, fmt.Errorf("failed to read first row: %v", err)
	}

	// Create schema with inferred types
	schema := make([]map[string]string, len(columns))
	for i, col := range columns {
		colType := "String" // Default type
		if row != nil && i < len(row) {
			val := row[i]
			if val != "" {
				// Try to infer type from the value
				if _, err := strconv.ParseInt(val, 10, 64); err == nil {
					colType = "Int64"
				} else if _, err := strconv.ParseFloat(val, 64); err == nil {
					colType = "Float64"
				} else if _, err := time.Parse("2006-01-02 15:04:05", val); err == nil {
					colType = "DateTime"
				}
			}
		}
		schema[i] = map[string]string{
			"name": col,
			"type": colType,
		}
	}

	return schema, nil
}

// PreviewData returns the first n rows of data
func PreviewData(conn driver.Conn, query string, limit int) ([]map[string]interface{}, error) {
	log.Printf("Executing query: %s", query)

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	columns := rows.Columns()
	columnTypes := rows.ColumnTypes()
	results := []map[string]interface{}{}

	log.Printf("Columns: %v", columns)
	log.Printf("Column types: %v", columnTypes)

	for rows.Next() {
		// Create a slice of pointers to scan into
		scanArgs := make([]interface{}, len(columns))
		for i := range scanArgs {
			// Get the column type
			colType := columnTypes[i].DatabaseTypeName()

			// Create appropriate type based on ClickHouse type
			switch colType {
			case "UInt8":
				var val uint8
				scanArgs[i] = &val
			case "UInt16":
				var val uint16
				scanArgs[i] = &val
			case "UInt32":
				var val uint32
				scanArgs[i] = &val
			case "UInt64":
				var val uint64
				scanArgs[i] = &val
			case "Int8":
				var val int8
				scanArgs[i] = &val
			case "Int16":
				var val int16
				scanArgs[i] = &val
			case "Int32":
				var val int32
				scanArgs[i] = &val
			case "Int64":
				var val int64
				scanArgs[i] = &val
			case "Float32":
				var val float32
				scanArgs[i] = &val
			case "Float64":
				var val float64
				scanArgs[i] = &val
			case "String":
				var val string
				scanArgs[i] = &val
			case "DateTime":
				var val time.Time
				scanArgs[i] = &val
			default:
				// For unknown types, use interface{}
				var val interface{}
				scanArgs[i] = &val
			}
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		// Create a map for the current row
		row := make(map[string]interface{})
		for i, col := range columns {
			// Dereference the pointer to get the actual value
			val := reflect.ValueOf(scanArgs[i]).Elem().Interface()

			// Convert the value to a string representation
			switch v := val.(type) {
			case time.Time:
				row[col] = v.Format(time.RFC3339)
			case nil:
				row[col] = nil
			default:
				row[col] = v
			}
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %v", err)
	}

	return results, nil
}

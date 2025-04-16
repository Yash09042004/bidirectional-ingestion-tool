package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// IngestDataFromClickHouseToFlatFile ingests data from ClickHouse to a flat file
func IngestDataFromClickHouseToFlatFile(conn driver.Conn, query, fileName, delimiter string) (int, error) {
	// Get the current working directory
	wd, err := os.Getwd()
	if err != nil {
		return 0, fmt.Errorf("failed to get working directory: %v", err)
	}

	// Create output directory if it doesn't exist
	outputDir := filepath.Join(wd, "output")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create output directory: %v", err)
	}

	// Use fixed output file name with absolute path
	filePath := filepath.Join(outputDir, "output.csv")
	log.Printf("Creating output file at: %s", filePath)

	// Remove existing file if it exists
	if _, err := os.Stat(filePath); err == nil {
		if err := os.Remove(filePath); err != nil {
			return 0, fmt.Errorf("failed to remove existing file: %v", err)
		}
	}

	file, err := os.Create(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to create file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Set the delimiter
	if delimiter == "" {
		delimiter = ","
	}
	writer.Comma = rune(delimiter[0])

	log.Printf("Executing query: %s", query)
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return 0, fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	// Get column types
	columnTypes := rows.ColumnTypes()
	scanArgs := make([]interface{}, len(columnTypes))
	for i, colType := range columnTypes {
		switch colType.DatabaseTypeName() {
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
			var val interface{}
			scanArgs[i] = &val
		}
	}

	// Write header
	columns := rows.Columns()
	log.Printf("Writing columns: %v", columns)
	if err := writer.Write(columns); err != nil {
		return 0, fmt.Errorf("failed to write header: %v", err)
	}

	// Write data
	recordCount := 0
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return recordCount, fmt.Errorf("failed to scan row: %v", err)
		}

		// Convert values to strings
		row := make([]string, len(columns))
		for i, val := range scanArgs {
			switch v := val.(type) {
			case *uint8:
				row[i] = fmt.Sprintf("%d", *v)
			case *uint16:
				row[i] = fmt.Sprintf("%d", *v)
			case *uint32:
				row[i] = fmt.Sprintf("%d", *v)
			case *uint64:
				row[i] = fmt.Sprintf("%d", *v)
			case *int8:
				row[i] = fmt.Sprintf("%d", *v)
			case *int16:
				row[i] = fmt.Sprintf("%d", *v)
			case *int32:
				row[i] = fmt.Sprintf("%d", *v)
			case *int64:
				row[i] = fmt.Sprintf("%d", *v)
			case *float32:
				row[i] = fmt.Sprintf("%f", *v)
			case *float64:
				row[i] = fmt.Sprintf("%f", *v)
			case *string:
				row[i] = *v
			case *time.Time:
				row[i] = v.Format("2006-01-02 15:04:05")
			case *interface{}:
				if *v == nil {
					row[i] = ""
				} else {
					row[i] = fmt.Sprintf("%v", *v)
				}
			default:
				row[i] = fmt.Sprintf("%v", val)
			}
		}

		if err := writer.Write(row); err != nil {
			return recordCount, fmt.Errorf("failed to write row: %v", err)
		}
		recordCount++
		log.Printf("Processed row %d: %v", recordCount, row)
	}

	if err := rows.Err(); err != nil {
		return recordCount, fmt.Errorf("error iterating rows: %v", err)
	}

	// Ensure all data is written to the file
	writer.Flush()
	if err := writer.Error(); err != nil {
		return recordCount, fmt.Errorf("error flushing writer: %v", err)
	}

	log.Printf("Successfully processed %d records", recordCount)
	return recordCount, nil
}

// IngestDataFromFlatFileToClickHouse ingests data from a flat file to ClickHouse
func IngestDataFromFlatFileToClickHouse(conn driver.Conn, fileName, delimiter, tableName string) (int, error) {
	// Get the current working directory
	wd, err := os.Getwd()
	if err != nil {
		return 0, fmt.Errorf("failed to get working directory: %v", err)
	}

	// Create the full file path
	filePath := filepath.Join(wd, fileName)
	log.Printf("Reading input file from: %s", filePath)

	// Open the input file
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %v", err)
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
		return 0, fmt.Errorf("failed to read header: %v", err)
	}
	log.Printf("Found columns: %v", columns)

	// Get column types from ClickHouse
	columnTypesQuery := fmt.Sprintf("DESCRIBE TABLE %s", tableName)
	rows, err := conn.Query(context.Background(), columnTypesQuery)
	if err != nil {
		return 0, fmt.Errorf("failed to get column types: %v", err)
	}
	defer rows.Close()

	// Map column names to their types
	columnTypes := make(map[string]string)
	for rows.Next() {
		var name, typeStr, defaultType, defaultExpr, comment string
		if err := rows.Scan(&name, &typeStr, &defaultType, &defaultExpr, &comment); err != nil {
			return 0, fmt.Errorf("failed to scan column type: %v", err)
		}
		columnTypes[name] = typeStr
	}

	// Prepare the insert statement
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	log.Printf("Preparing insert statement: %s", query)
	stmt, err := conn.PrepareBatch(context.Background(), query)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare batch: %v", err)
	}

	// Process rows
	recordCount := 0
	for {
		row, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return recordCount, fmt.Errorf("failed to read row: %v", err)
		}

		// Convert values based on column types
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			colType := columnTypes[col]
			val := row[i]

			switch colType {
			case "UInt8", "UInt16", "UInt32", "UInt64":
				if val == "" {
					values[i] = uint64(0)
				} else {
					v, err := strconv.ParseUint(val, 10, 64)
					if err != nil {
						return recordCount, fmt.Errorf("failed to parse uint for column %s: %v", col, err)
					}
					values[i] = v
				}
			case "Int8", "Int16", "Int32", "Int64":
				if val == "" {
					values[i] = int64(0)
				} else {
					v, err := strconv.ParseInt(val, 10, 64)
					if err != nil {
						return recordCount, fmt.Errorf("failed to parse int for column %s: %v", col, err)
					}
					values[i] = v
				}
			case "Float32", "Float64":
				if val == "" {
					values[i] = float64(0)
				} else {
					v, err := strconv.ParseFloat(val, 64)
					if err != nil {
						return recordCount, fmt.Errorf("failed to parse float for column %s: %v", col, err)
					}
					values[i] = v
				}
			case "DateTime":
				if val == "" {
					values[i] = time.Time{}
				} else {
					v, err := time.Parse("2006-01-02 15:04:05", val)
					if err != nil {
						return recordCount, fmt.Errorf("failed to parse datetime for column %s: %v", col, err)
					}
					values[i] = v
				}
			default:
				values[i] = val
			}
		}

		if err := stmt.Append(values...); err != nil {
			return recordCount, fmt.Errorf("failed to append row: %v", err)
		}

		recordCount++
		if recordCount%1000 == 0 {
			log.Printf("Processed %d records", recordCount)
		}
	}

	// Execute the batch
	if err := stmt.Send(); err != nil {
		return recordCount, fmt.Errorf("failed to send batch: %v", err)
	}

	log.Printf("Successfully processed %d records", recordCount)
	return recordCount, nil
}

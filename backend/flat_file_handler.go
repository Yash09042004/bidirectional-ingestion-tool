package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// ReadFlatFile reads data from a flat file with the specified delimiter
func ReadFlatFile(fileName, delimiter string) ([][]string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	var data [][]string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, delimiter)
		data = append(data, fields)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	return data, nil
}

// WriteFlatFile writes data to a flat file with the specified delimiter
func WriteFlatFile(fileName, delimiter string, data [][]string) error {
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, record := range data {
		line := strings.Join(record, delimiter)
		if _, err := writer.WriteString(line + "\n"); err != nil {
			return fmt.Errorf("error writing to file: %w", err)
		}
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("error flushing writer: %w", err)
	}

	return nil
} 
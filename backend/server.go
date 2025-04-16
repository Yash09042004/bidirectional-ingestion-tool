package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gorilla/mux"
)

// IngestionRequest represents the request body for data ingestion.
type IngestionRequest struct {
	Source           string            `json:"source"`
	ClickHouseConfig map[string]string `json:"clickHouseConfig"`
	FlatFileConfig   map[string]string `json:"flatFileConfig"`
	SelectedColumns  []string          `json:"selectedColumns"`
}

type SchemaRequest struct {
	Source           string            `json:"source"`
	ClickHouseConfig map[string]string `json:"clickHouseConfig"`
	FlatFileConfig   map[string]string `json:"flatFileConfig"`
}

type PreviewRequest struct {
	Source           string            `json:"source"`
	ClickHouseConfig map[string]string `json:"clickHouseConfig"`
	FlatFileConfig   map[string]string `json:"flatFileConfig"`
	TableName        string            `json:"tableName"`
	Columns          []string          `json:"columns"`
}

func ingestHandler(w http.ResponseWriter, r *http.Request) {
	var req IngestionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Error decoding request: %v", err)
		http.Error(w, jsonError("Invalid request payload"), http.StatusBadRequest)
		return
	}

	// Validate required fields
	if req.Source != "clickhouse" && req.Source != "flatfile" {
		log.Printf("Invalid source type: %s", req.Source)
		http.Error(w, jsonError("Invalid source type"), http.StatusBadRequest)
		return
	}

	if req.Source == "clickhouse" {
		if req.ClickHouseConfig["host"] == "" || req.ClickHouseConfig["port"] == "" ||
			req.ClickHouseConfig["database"] == "" || req.ClickHouseConfig["user"] == "" {
			log.Printf("Missing required ClickHouse configuration")
			http.Error(w, jsonError("Missing required ClickHouse configuration"), http.StatusBadRequest)
			return
		}
	} else {
		if req.FlatFileConfig["fileName"] == "" {
			log.Printf("Missing required file name")
			http.Error(w, jsonError("Missing required file name"), http.StatusBadRequest)
			return
		}
	}

	if len(req.SelectedColumns) == 0 {
		log.Printf("No columns selected")
		http.Error(w, jsonError("No columns selected"), http.StatusBadRequest)
		return
	}

	// Create output directory if it doesn't exist
	outputDir := "output"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Printf("Error creating output directory: %v", err)
		http.Error(w, jsonError("Failed to create output directory"), http.StatusInternalServerError)
		return
	}

	// Get absolute path for the output file
	wd, wdErr := os.Getwd()
	if wdErr != nil {
		log.Printf("Error getting working directory: %v", wdErr)
		http.Error(w, jsonError("Failed to get working directory"), http.StatusInternalServerError)
		return
	}

	filePath := filepath.Join(wd, outputDir, "output.csv")
	log.Printf("Output file path: %s", filePath)

	var recordCount int
	var ingestErr error

	if req.Source == "clickhouse" {
		conn, connErr := connectToClickHouse(
			req.ClickHouseConfig["host"],
			req.ClickHouseConfig["port"],
			req.ClickHouseConfig["database"],
			req.ClickHouseConfig["user"],
			req.ClickHouseConfig["jwtToken"],
		)
		if connErr != nil {
			log.Printf("Error connecting to ClickHouse: %v", connErr)
			http.Error(w, jsonError(fmt.Sprintf("Failed to connect to ClickHouse: %v", connErr)), http.StatusInternalServerError)
			return
		}
		defer conn.Close()

		// Clean column names by removing type information
		cleanColumns := make([]string, len(req.SelectedColumns))
		for i, col := range req.SelectedColumns {
			parts := strings.Split(col, " ")
			cleanColumns[i] = parts[0]
		}

		// Ensure table name is properly set
		tableName := req.ClickHouseConfig["table"]
		if tableName == "" {
			log.Printf("Missing table name in ClickHouse configuration")
			http.Error(w, jsonError("Missing table name in ClickHouse configuration"), http.StatusBadRequest)
			return
		}

		query := fmt.Sprintf("SELECT %s FROM %s.%s",
			strings.Join(cleanColumns, ", "),
			req.ClickHouseConfig["database"],
			tableName)
		log.Printf("Executing ingestion query: %s", query)
		recordCount, ingestErr = IngestDataFromClickHouseToFlatFile(conn, query, filePath, req.FlatFileConfig["delimiter"])
	} else {
		if req.ClickHouseConfig["table"] == "" {
			log.Printf("Missing target table name in ClickHouse configuration")
			http.Error(w, jsonError("Missing target table name in ClickHouse configuration"), http.StatusBadRequest)
			return
		}
		conn, connErr := connectToClickHouse(
			req.ClickHouseConfig["host"],
			req.ClickHouseConfig["port"],
			req.ClickHouseConfig["database"],
			req.ClickHouseConfig["user"],
			req.ClickHouseConfig["jwtToken"],
		)
		if connErr != nil {
			log.Printf("Error connecting to ClickHouse: %v", connErr)
			http.Error(w, jsonError(fmt.Sprintf("Failed to connect to ClickHouse: %v", connErr)), http.StatusInternalServerError)
			return
		}
		defer conn.Close()

		recordCount, ingestErr = IngestDataFromFlatFileToClickHouse(conn, req.FlatFileConfig["fileName"], req.FlatFileConfig["delimiter"], req.ClickHouseConfig["table"])
	}

	if ingestErr != nil {
		log.Printf("Error during ingestion: %v", ingestErr)
		http.Error(w, jsonError(fmt.Sprintf("Failed to ingest data: %v", ingestErr)), http.StatusInternalServerError)
		return
	}

	// Return the record count and absolute file path in the response
	response := map[string]interface{}{
		"status":      "success",
		"recordCount": recordCount,
		"outputFile":  filePath,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding response: %v", err)
		http.Error(w, jsonError("Failed to encode response"), http.StatusInternalServerError)
		return
	}
}

func schemaHandler(w http.ResponseWriter, r *http.Request) {
	var req SchemaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Error decoding request: %v", err)
		http.Error(w, jsonError("Invalid request format"), http.StatusBadRequest)
		return
	}

	if req.Source != "ClickHouse" && req.Source != "FlatFile" {
		log.Printf("Invalid source type: %s", req.Source)
		http.Error(w, jsonError("Invalid source type"), http.StatusBadRequest)
		return
	}

	var result interface{}
	var err error

	if req.Source == "ClickHouse" {
		if req.ClickHouseConfig["host"] == "" || req.ClickHouseConfig["port"] == "" ||
			req.ClickHouseConfig["database"] == "" || req.ClickHouseConfig["user"] == "" {
			log.Printf("Missing required ClickHouse configuration")
			http.Error(w, jsonError("Missing required ClickHouse configuration"), http.StatusBadRequest)
			return
		}

		conn, err := connectToClickHouse(
			req.ClickHouseConfig["host"],
			req.ClickHouseConfig["port"],
			req.ClickHouseConfig["database"],
			req.ClickHouseConfig["user"],
			req.ClickHouseConfig["jwtToken"],
		)
		if err != nil {
			log.Printf("Error connecting to ClickHouse: %v", err)
			http.Error(w, jsonError(fmt.Sprintf("Failed to connect to ClickHouse: %v", err)), http.StatusInternalServerError)
			return
		}
		defer conn.Close()

		result, err = GetClickHouseTables(conn)
	} else {
		if req.FlatFileConfig["fileName"] == "" {
			log.Printf("Missing required file name")
			http.Error(w, jsonError("Missing required file name"), http.StatusBadRequest)
			return
		}

		// Get the current working directory
		wd, wdErr := os.Getwd()
		if wdErr != nil {
			log.Printf("Error getting working directory: %v", wdErr)
			http.Error(w, jsonError("Failed to get working directory"), http.StatusInternalServerError)
			return
		}

		// Create the full file path
		filePath := filepath.Join(wd, req.FlatFileConfig["fileName"])
		log.Printf("Reading schema from file: %s", filePath)

		// Check if file exists
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			log.Printf("File does not exist: %s", filePath)
			http.Error(w, jsonError("File does not exist"), http.StatusBadRequest)
			return
		}

		result, err = GetFlatFileSchema(req.FlatFileConfig["fileName"], req.FlatFileConfig["delimiter"])
	}

	if err != nil {
		log.Printf("Error getting schema: %v", err)
		http.Error(w, jsonError(fmt.Sprintf("Failed to get schema: %v", err)), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		log.Printf("Error encoding response: %v", err)
		http.Error(w, jsonError("Failed to encode response"), http.StatusInternalServerError)
		return
	}
}

func previewHandler(w http.ResponseWriter, r *http.Request) {
	var req PreviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Error decoding request: %v", err)
		http.Error(w, jsonError("Invalid request payload"), http.StatusBadRequest)
		return
	}

	log.Printf("Received preview request: %+v", req)

	if req.Source != "ClickHouse" && req.Source != "FlatFile" {
		log.Printf("Invalid source type: %s", req.Source)
		http.Error(w, jsonError("Invalid source type"), http.StatusBadRequest)
		return
	}

	if req.Source == "ClickHouse" {
		if req.ClickHouseConfig["host"] == "" || req.ClickHouseConfig["port"] == "" ||
			req.ClickHouseConfig["database"] == "" || req.ClickHouseConfig["user"] == "" {
			log.Printf("Missing required ClickHouse configuration")
			http.Error(w, jsonError("Missing required ClickHouse configuration"), http.StatusBadRequest)
			return
		}

		if req.TableName == "" || len(req.Columns) == 0 {
			log.Printf("Missing table name or columns")
			http.Error(w, jsonError("Missing table name or columns"), http.StatusBadRequest)
			return
		}

		conn, err := connectToClickHouse(
			req.ClickHouseConfig["host"],
			req.ClickHouseConfig["port"],
			req.ClickHouseConfig["database"],
			req.ClickHouseConfig["user"],
			req.ClickHouseConfig["jwtToken"],
		)
		if err != nil {
			log.Printf("Error connecting to ClickHouse: %v", err)
			http.Error(w, jsonError(fmt.Sprintf("Failed to connect to ClickHouse: %v", err)), http.StatusInternalServerError)
			return
		}
		defer conn.Close()

		// Clean column names by removing type information
		cleanColumns := make([]string, len(req.Columns))
		for i, col := range req.Columns {
			parts := strings.Split(col, " ")
			cleanColumns[i] = parts[0]
		}

		// Build the query with proper database and table references
		query := fmt.Sprintf("SELECT %s FROM %s.%s LIMIT 100",
			strings.Join(cleanColumns, ", "),
			req.ClickHouseConfig["database"],
			req.TableName)

		log.Printf("Executing preview query: %s", query)
		preview, err := PreviewData(conn, query, 100)
		if err != nil {
			log.Printf("Error executing preview query: %v", err)
			http.Error(w, jsonError(fmt.Sprintf("Failed to preview data: %v", err)), http.StatusInternalServerError)
			return
		}

		log.Printf("Preview data retrieved: %d rows", len(preview))

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(preview); err != nil {
			log.Printf("Error encoding response: %v", err)
			http.Error(w, jsonError("Failed to encode response"), http.StatusInternalServerError)
			return
		}
	} else {
		// Handle flat file preview
		if req.FlatFileConfig["fileName"] == "" {
			log.Printf("Missing required file name")
			http.Error(w, jsonError("Missing required file name"), http.StatusBadRequest)
			return
		}

		// Get the current working directory
		wd, wdErr := os.Getwd()
		if wdErr != nil {
			log.Printf("Error getting working directory: %v", wdErr)
			http.Error(w, jsonError("Failed to get working directory"), http.StatusInternalServerError)
			return
		}

		// Create the full file path
		filePath := filepath.Join(wd, req.FlatFileConfig["fileName"])
		log.Printf("Reading preview from file: %s", filePath)

		// Check if file exists
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			log.Printf("File does not exist: %s", filePath)
			http.Error(w, jsonError("File does not exist"), http.StatusBadRequest)
			return
		}

		// Open the file
		file, err := os.Open(filePath)
		if err != nil {
			log.Printf("Error opening file: %v", err)
			http.Error(w, jsonError(fmt.Sprintf("Failed to open file: %v", err)), http.StatusInternalServerError)
			return
		}
		defer file.Close()

		// Create CSV reader
		reader := csv.NewReader(file)
		if req.FlatFileConfig["delimiter"] != "" {
			reader.Comma = rune(req.FlatFileConfig["delimiter"][0])
		}

		// Read header
		header, err := reader.Read()
		if err != nil {
			log.Printf("Error reading header: %v", err)
			http.Error(w, jsonError(fmt.Sprintf("Failed to read header: %v", err)), http.StatusInternalServerError)
			return
		}

		// Get column indices for selected columns
		columnIndices := make(map[string]int)
		for i, col := range header {
			columnIndices[col] = i
		}

		// Read up to 100 rows
		var preview []map[string]interface{}
		for i := 0; i < 100; i++ {
			row, err := reader.Read()
			if err != nil {
				if err.Error() == "EOF" {
					break
				}
				log.Printf("Error reading row: %v", err)
				continue
			}

			// Create a map for the current row
			rowMap := make(map[string]interface{})
			for _, col := range req.Columns {
				// Clean column name by removing type information
				parts := strings.Split(col, " ")
				colName := parts[0]
				if idx, ok := columnIndices[colName]; ok && idx < len(row) {
					rowMap[col] = row[idx]
				}
			}
			preview = append(preview, rowMap)
		}

		log.Printf("Preview data retrieved: %d rows", len(preview))

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(preview); err != nil {
			log.Printf("Error encoding response: %v", err)
			http.Error(w, jsonError("Failed to encode response"), http.StatusInternalServerError)
			return
		}
	}
}

func enableCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Allow requests from both development ports
		origin := r.Header.Get("Origin")
		if origin == "http://localhost:3000" || origin == "http://localhost:3001" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Credentials", "true")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func jsonError(message string) string {
	errorJSON, _ := json.Marshal(map[string]string{"error": message})
	return string(errorJSON)
}

func main() {
	r := mux.NewRouter()
	r.Use(enableCORS)
	r.HandleFunc("/schema", schemaHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/preview", previewHandler).Methods("POST", "OPTIONS")
	r.HandleFunc("/ingest", ingestHandler).Methods("POST", "OPTIONS")

	port := os.Getenv("PORT")
	if port == "" {
		port = "5000"
	}

	log.Printf("Server starting on port %s...", port)
	log.Fatal(http.ListenAndServe(":"+port, r))
}

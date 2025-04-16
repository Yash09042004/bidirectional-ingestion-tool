# Bidirectional ClickHouse & Flat File Data Ingestion Tool

## Overview

This project implements a web-based tool that enables bidirectional data ingestion between a ClickHouse database and flat files (e.g., CSV). The tool supports:

- **ClickHouse → Flat File**: Export selected data from ClickHouse to a flat file.
- **Flat File → ClickHouse**: Import data from a flat file into ClickHouse.

---

## Features

- **Bidirectional Data Flow**: Export from ClickHouse to flat file and import from flat file to ClickHouse.
- **JWT Authentication**: Secure connection to ClickHouse using JWT tokens.
- **Column Selection**: Choose specific columns for ingestion.
- **Progress and Status Reporting**: Real-time status updates and final record count upon completion.
- **Error Handling**: User-friendly error messages for connection and processing issues.

---

## Tech Stack

### Backend
- **Language**: Go
- **Framework**: Standard Go HTTP server
- **Database Client**: Official ClickHouse client for Go
- **Authentication**: JWT token-based authentication

### Frontend
- **Framework**: React
- **HTTP Client**: Axios for backend communication

---

## Prerequisites

- **Go** 1.16+
- **Node.js** (for the React frontend)
- **Docker**: For running a local ClickHouse instance

---

## Setup & Installation

### 1. ClickHouse Setup
Run ClickHouse locally using Docker:
```bash
docker run -d --name clickhouse-server --ulimit nofile=262144:262144 -p 8123:8123 clickhouse/clickhouse-server
```

Verify the ClickHouse server is running:
```bash
docker ps
```

### 2. Backend Setup
Navigate to the backend directory:
```bash
cd /path/to/backend
```

Install Go dependencies:
```bash
go mod tidy
```

Start the backend server:
```bash
go run .
```

The backend will start on the configured port (e.g., `http://localhost:8080`).

### 3. Frontend Setup
Navigate to the frontend directory:
```bash
cd /path/to/frontend
```

Install Node.js dependencies:
```bash
npm install
```

Start the frontend development server:
```bash
npm start
```

The UI should be accessible at `http://localhost:3000`.

---

## Usage

### Source Selection:
- Use the UI to select the data source (ClickHouse or Flat File).

### Provide Connection Details:
- For ClickHouse, enter Host, Port, Database, User, and JWT Token.
- For Flat File, specify the file name and delimiter used.

### Schema Discovery:
- Click “Load Columns” to list available tables (for ClickHouse) or infer flat file schema.
- Select the columns you want to ingest.

### Data Ingestion:
- Click the “Start Ingestion” button. The system will process the data and display status updates and the final record count.

---

## Project Structure

- **`backend/`**: Contains the Go backend code.
- **`frontend/`**: Contains the React frontend code.
- **`tests/`**: Includes unit tests for the backend and frontend.

---

## Testing

- **ClickHouse to Flat File**: Verify exported flat file contains correct and selected data from ClickHouse.
- **Flat File to ClickHouse**: Check the target table in ClickHouse to ensure all records are imported correctly.
- **Error Handling**: Test with wrong credentials or incorrect file formats to see appropriate error messages.

---

## Final Remarks

This tool addresses the need for a robust, bidirectional ingestion system between ClickHouse and flat files, integrating modern authentication and usability features. For any issues or contributions, please open an issue or pull request on GitHub.
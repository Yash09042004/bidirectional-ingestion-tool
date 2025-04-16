import React, { useState } from 'react';
import SchemaSelector from './components/SchemaSelector';
import DataPreview from './components/DataPreview';
import './App.css';

function App() {
  const [source, setSource] = useState('ClickHouse');
  const [clickHouseConfig, setClickHouseConfig] = useState({
    host: 'localhost',
    port: '9000',
    database: 'ingestion_db',
    user: 'default',
    jwtToken: '',
  });
  const [flatFileConfig, setFlatFileConfig] = useState({
    fileName: '',
    delimiter: ',',
  });
  const [selectedTable, setSelectedTable] = useState('');
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [status, setStatus] = useState('');
  const [recordCount, setRecordCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [targetTable, setTargetTable] = useState("");

  const handleSourceChange = (e) => {
    setSource(e.target.value);
    setSelectedTable('');
    setSelectedColumns([]);
    setStatus('');
    setError(null);
  };

  const handleConfigChange = (e, configType) => {
    const { name, value } = e.target;
    if (configType === 'clickHouse') {
      setClickHouseConfig(prev => ({ ...prev, [name]: value }));
    } else {
      setFlatFileConfig(prev => ({ ...prev, [name]: value }));
    }
    setStatus('');
    setError(null);
  };

  const handleSchemaChange = (tableName, columns) => {
    setSelectedTable(tableName);
    setSelectedColumns(columns);
    setStatus('');
    setError(null);
  };

  const handleTargetTableChange = (e) => {
    setTargetTable(e.target.value);
    setStatus("");
    setError(null);
  };

  const validateConfig = () => {
    if (source === 'ClickHouse') {
      if (!clickHouseConfig.host || !clickHouseConfig.port || 
          !clickHouseConfig.database || !clickHouseConfig.user) {
        setError('Missing required ClickHouse configuration');
        return false;
      }
    } else {
      if (!flatFileConfig.fileName) {
        setError('Missing required file name');
        return false;
      }
    }
    return true;
  };

  const handleIngest = async () => {
    if (!validateConfig()) {
      return;
    }

    if (
      (source === "ClickHouse" && (!selectedTable || selectedColumns.length === 0)) ||
      (source === "FlatFile" && (selectedColumns.length === 0 || !targetTable))
    ) {
      setError("Please select the required fields first");
      return;
    }

    setStatus("Starting ingestion...");
    setLoading(true);
    setError(null);

    try {
      const requestBody = {
        source: source.toLowerCase(),
        clickHouseConfig: {
          host: clickHouseConfig.host,
          port: clickHouseConfig.port,
          database: clickHouseConfig.database,
          user: clickHouseConfig.user,
          jwtToken: clickHouseConfig.jwtToken,
          table: source === "ClickHouse" ? selectedTable : targetTable
        },
        flatFileConfig: {
          fileName: flatFileConfig.fileName,
          delimiter: flatFileConfig.delimiter
        },
        selectedColumns: selectedColumns
      };

      console.log("Sending ingestion request with body:", requestBody);

      const response = await fetch("http://localhost:5000/ingest", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(requestBody),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || "Failed to ingest data");
      }

      const data = await response.json();
      setStatus(`Ingestion completed successfully. ${data.recordCount} records processed. Output file: ${data.outputFile}`);
      setRecordCount(data.recordCount);
    } catch (error) {
      console.error("Error during ingestion:", error);
      setError(error.message);
      setStatus('');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="App">
      <h1>Bidirectional Data Ingestion Tool</h1>
      
      <div className="source-selection">
        <h2>Select Source</h2>
        <select value={source} onChange={handleSourceChange}>
          <option value="ClickHouse">ClickHouse</option>
          <option value="FlatFile">Flat File</option>
        </select>
      </div>

      <div className="config-section">
        <h2>Configuration</h2>
        {source === 'ClickHouse' ? (
          <div className="clickhouse-config">
            <input
              type="text"
              name="host"
              placeholder="Host"
              value={clickHouseConfig.host}
              onChange={(e) => handleConfigChange(e, 'clickHouse')}
            />
            <input
              type="text"
              name="port"
              placeholder="Port"
              value={clickHouseConfig.port}
              onChange={(e) => handleConfigChange(e, 'clickHouse')}
            />
            <input
              type="text"
              name="database"
              placeholder="Database"
              value={clickHouseConfig.database}
              onChange={(e) => handleConfigChange(e, 'clickHouse')}
            />
            <input
              type="text"
              name="user"
              placeholder="Username"
              value={clickHouseConfig.user}
              onChange={(e) => handleConfigChange(e, 'clickHouse')}
            />
            <input
              type="password"
              name="jwtToken"
              placeholder="JWT Token"
              value={clickHouseConfig.jwtToken}
              onChange={(e) => handleConfigChange(e, 'clickHouse')}
            />
          </div>
        ) : (
          <div className="flatfile-config">
            <input
              type="text"
              name="fileName"
              placeholder="File Path"
              value={flatFileConfig.fileName}
              onChange={(e) => handleConfigChange(e, 'flatFile')}
            />
            <input
              type="text"
              name="delimiter"
              placeholder="Delimiter"
              value={flatFileConfig.delimiter}
              onChange={(e) => handleConfigChange(e, 'flatFile')}
            />
            <input
              type="text"
              name="targetTable"
              placeholder="Target Table Name in ClickHouse"
              value={targetTable}
              onChange={handleTargetTableChange}
            />
          </div>
        )}
      </div>

      {error && <div className="error">Error: {error}</div>}

      <div className="schema-section">
        <SchemaSelector
          source={source}
          config={source === 'ClickHouse' ? clickHouseConfig : flatFileConfig}
          onSchemaChange={handleSchemaChange}
        />
      </div>

      <div className="preview-section">
        <DataPreview
          source={source}
          config={source === 'ClickHouse' ? clickHouseConfig : flatFileConfig}
          tableName={selectedTable}
          columns={selectedColumns}
        />
      </div>

      <div className="action-section">
        <button
          onClick={handleIngest}
          // disabled={!selectedTable || selectedColumns.length === 0 ||  }
        >
          {loading ? 'Processing...' : 'Start Ingestion'}
        </button>
      </div>

      <div className="status-section">
        {status && <h3>Status: {status}</h3>}
        {recordCount > 0 && <p>Records processed: {recordCount}</p>}
      </div>
    </div>
  );
}

export default App; 
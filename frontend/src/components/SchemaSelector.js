import React, { useState, useEffect } from 'react';

const SchemaSelector = ({ source, config, onSchemaChange }) => {
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState('');
  const [columns, setColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [previewData, setPreviewData] = useState([]);

  const validateConfig = () => {
    if (!config) {
      setError('Configuration is required');
      return false;
    }

    if (source === 'ClickHouse') {
      if (!config.host || !config.port || !config.database || !config.user) {
        setError('Missing required ClickHouse configuration (host, port, database, user)');
        return false;
      }
    } else if (source === 'FlatFile') {
      if (!config.fileName) {
        setError('Missing required file name');
        return false;
      }
    }
    return true;
  };

  const fetchPreviewData = async () => {
    if (!selectedColumns.length) {
      setPreviewData([]);
      return;
    }

    try {
      const response = await fetch('http://localhost:5000/preview', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          source,
          clickHouseConfig: source === 'ClickHouse' ? config : {},
          flatFileConfig: source === 'FlatFile' ? config : {},
          table: selectedTable,
          columns: selectedColumns,
        }),
      });

      if (!response.ok) {
        throw new Error('Failed to fetch preview data');
      }

      const data = await response.json();
      setPreviewData(data);
    } catch (err) {
      console.error('Error fetching preview data:', err);
      setPreviewData([]);
    }
  };

  useEffect(() => {
    const fetchSchema = async () => {
      if (!validateConfig()) {
        return;
      }

      setLoading(true);
      setError(null);
      try {
        const response = await fetch('http://localhost:5000/schema', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            source,
            clickHouseConfig: source === 'ClickHouse' ? config : {},
            flatFileConfig: source === 'FlatFile' ? config : {},
          }),
        });

        if (!response.ok) {
          const errorData = await response.json();
          throw new Error(errorData.error || 'Failed to fetch schema');
        }

        const data = await response.json();
        
        if (source === 'ClickHouse') {
          setTables(data);
          setColumns([]);
          setSelectedTable('');
          setSelectedColumns([]);
        } else {
          // For FlatFile, data is an array of column objects
          setTables([]);
          setColumns(data.map(col => `${col.name} (${col.type})`));
          setSelectedTable('');
          setSelectedColumns([]);
        }
      } catch (err) {
        setError(err.message);
        console.error('Error fetching schema:', err);
      } finally {
        setLoading(false);
      }
    };

    if (source && config) {
      fetchSchema();
    }
  }, [source, config]);

  useEffect(() => {
    if (selectedColumns.length > 0) {
      fetchPreviewData();
    } else {
      setPreviewData([]);
    }
  }, [selectedColumns, selectedTable]);

  const handleTableChange = (e) => {
    const tableName = e.target.value;
    setSelectedTable(tableName);
    setSelectedColumns([]);
    
    if (source === 'ClickHouse') {
      const selectedTableData = tables.find(t => t.name === tableName);
      if (selectedTableData) {
        setColumns(selectedTableData.columns);
      }
    }
  };

  const handleColumnChange = (column) => {
    const newSelectedColumns = selectedColumns.includes(column)
      ? selectedColumns.filter(c => c !== column)
      : [...selectedColumns, column];
    setSelectedColumns(newSelectedColumns);
    onSchemaChange(selectedTable, newSelectedColumns);
  };

  return (
    <div className="schema-selector">
      {loading && <p>Loading schema...</p>}
      {error && <p className="error">{error}</p>}
      
      {source === 'ClickHouse' && (
        <div className="form-group">
          <label>Select Table:</label>
          <select value={selectedTable} onChange={handleTableChange}>
            <option value="">Select a table</option>
            {tables.map((table) => (
              <option key={table.name} value={table.name}>
                {table.name}
              </option>
            ))}
          </select>
        </div>
      )}

      {columns.length > 0 && (
        <div className="form-group">
          <label>Select Columns:</label>
          <div className="columns-list">
            {columns.map((column) => (
              <label key={column} className="column-checkbox">
                <input
                  type="checkbox"
                  checked={selectedColumns.includes(column)}
                  onChange={() => handleColumnChange(column)}
                />
                {column}
              </label>
            ))}
          </div>
        </div>
      )}

      {previewData.length > 0 && (
        <div className="preview-section">
          <h3>Preview Data</h3>
          <div className="preview-table">
            <table>
              <thead>
                <tr>
                  {selectedColumns.map((column) => (
                    <th key={column}>{column}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {previewData.map((row, index) => (
                  <tr key={index}>
                    {selectedColumns.map((column) => (
                      <td key={`${index}-${column}`}>{row[column]}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <style>
        {`
          .columns-list {
            display: flex;
            flex-direction: column;
            gap: 8px;
            max-height: 300px;
            overflow-y: auto;
            padding: 8px;
            border: 1px solid #ccc;
            border-radius: 4px;
          }
          .column-checkbox {
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 4px;
            cursor: pointer;
          }
          .column-checkbox:hover {
            background-color: #f5f5f5;
          }
          .form-group {
            margin-bottom: 16px;
          }
          .form-group label {
            display: block;
            margin-bottom: 8px;
            font-weight: bold;
          }
          select {
            width: 100%;
            padding: 8px;
            border: 1px solid #ccc;
            border-radius: 4px;
          }
          .error {
            color: red;
            margin-bottom: 16px;
          }
          .preview-section {
            margin-top: 20px;
          }
          .preview-table {
            max-height: 400px;
            overflow-y: auto;
            border: 1px solid #ccc;
            border-radius: 4px;
          }
          table {
            width: 100%;
            border-collapse: collapse;
          }
          th, td {
            padding: 8px;
            border: 1px solid #ddd;
            text-align: left;
          }
          th {
            background-color: #f5f5f5;
            position: sticky;
            top: 0;
          }
          tr:nth-child(even) {
            background-color: #f9f9f9;
          }
        `}
      </style>
    </div>
  );
};

export default SchemaSelector; 
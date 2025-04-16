import React, { useState, useEffect } from 'react';

function DataPreview({ source, config, tableName, columns }) {
  const [previewData, setPreviewData] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (tableName && columns.length > 0) {
      fetchPreview();
    } else {
      setPreviewData([]);
      setError(null);
    }
  }, [tableName, columns, config, source]);

  const validateConfig = () => {
    if (source === 'ClickHouse') {
      if (!config.host || !config.port || !config.database || !config.user) {
        setError('Missing required ClickHouse configuration');
        return false;
      }
    } else {
      if (!config.fileName) {
        setError('Missing required file name');
        return false;
      }
    }
    return true;
  };

  const fetchPreview = async () => {
    if (!validateConfig()) {
      return;
    }

    try {
      setLoading(true);
      setError(null);
      console.log('Fetching preview with config:', {
        source,
        config,
        tableName,
        columns
      });

      const requestBody = {
        source,
        tableName,
        columns,
        clickHouseConfig: source === 'ClickHouse' ? {
          host: config.host,
          port: config.port,
          database: config.database,
          user: config.user,
          jwtToken: config.jwtToken
        } : {},
        flatFileConfig: source === 'FlatFile' ? {
          fileName: config.fileName,
          delimiter: config.delimiter
        } : {}
      };

      console.log('Sending request with body:', requestBody);

      const response = await fetch('http://localhost:5000/preview', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(requestBody)
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || `HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      console.log('Received preview data:', data);
      
      if (!Array.isArray(data)) {
        throw new Error('Invalid response format from server');
      }
      
      setPreviewData(data);
    } catch (error) {
      console.error('Error fetching preview:', error);
      setError(error.message);
      setPreviewData([]);
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return <div>Loading preview...</div>;
  }

  if (error) {
    return <div className="error">Error: {error}</div>;
  }

  if (!tableName || columns.length === 0) {
    return <div>Select a table and columns to preview data</div>;
  }

  if (previewData.length === 0) {
    return <div>No data available for preview</div>;
  }

  // Clean column names by removing type information
  const cleanColumns = columns.map(col => {
    const parts = col.split(' ');
    return parts[0];
  });

  return (
    <div className="preview">
      <h3>Data Preview</h3>
      <table>
        <thead>
          <tr>
            {cleanColumns.map((column) => (
              <th key={column}>{column}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {previewData.map((row, index) => (
            <tr key={index}>
              {cleanColumns.map((column) => (
                <td key={`${column}-${index}`}>{row[column]}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default DataPreview;

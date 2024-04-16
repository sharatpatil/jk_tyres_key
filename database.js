const sql = require('mssql');
const os = require('os');

let pool; // Declare the pool as a global variable for reuse

function connectToDatabase() {
  const config = {
    user: 'sa',
    password: 'sa123',
    server: `${os.hostname}\\SQLSERVER`,
    database: 'test',
    options: {
      encrypt: false,
    },
  };

  const connection = new sql.ConnectionPool(config);

  // Set the isolation level here
  connection.on('connect', () => {
    const request = connection.request();
    request.query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED", (err) => {
      if (err) {
        console.error("Error setting isolation level:", err);
      }
    });
  });

  return connection.connect();
}

module.exports = {
  connectToDatabase,
  setPool: (newPool) => {
    pool = newPool;
  },
};

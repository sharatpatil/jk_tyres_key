const os = require('os');

const config = {
  user: 'sa',
  password: 'sa123',
  server: `${os.hostname}\\SQLSERVER`,
  database: 'test',
  options: {
    encrypt: false,
  },
};

module.exports = { config };

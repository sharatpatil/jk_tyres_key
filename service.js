const Service = require('node-windows').Service;

// Create a new service object
const svc = new Service({
  name: 'JK tyres api V1.0',
  description: 'JK tyres api running at 4000',
  script: 'D:\\nodejs\\jk_tyres_api\\index.js', // Replace with the full path to your index.js file
});

// Listen for the "install" event
svc.on('install', () => {
  // Start the service after installation
  svc.start();
});

// Listen for the "uninstall" event
svc.on('uninstall', () => {
  console.log('Uninstalling JK tyres api service...');
  // Stop the service and uninstall it
  svc.stop();
  svc.uninstall();
});

// Check if the script is run with the "uninstall" argument
if (process.argv[2] === 'uninstall') {
  // Trigger the uninstall event
  svc.emit('uninstall');
} else {
  // Install the service
  svc.install();
}

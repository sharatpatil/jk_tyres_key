const PORT = 3001;
const os = require('os');

const express = require('express');
const sql = require('mssql');
const axios = require('axios');
const cron = require('node-cron');
const https = require('https');
const cluster = require('cluster');
const http = require('http');

const app = express();

const headers = {
  'Content-Type': 'application/json',
  'X-Access-Key': '8a31ef8btf65c31tc15dt32e888c3ab731e919ac'
};

const apiUrl = 'https://api.datonis.io/api/v3/datonis_query/thing_data';
const pageSize = 100; // Number of records per page
let currentPage = 1;
let totalRecords = 0;
let totalPages = 0;

// let thing_key ='efeded6tbb';

// const thingKeys = ['efeded6tbb'];

 const thingKeys = ['2cdc143f43','8a921t3t2d','52ac146b26','47799b3af4','bffcdd11ef','4tfa3b765b','5c58d23t67','ee9e4a62ea','18ac35cfde','c25beddt44','efeded6tbb','9ed1e149a1','c3a54419fc','86f8ada652','f1d597831d','193193d481','794ecat31d','a9t2bb36f7','527443tbe1','f5a9551d7a']


const config = {
  user: 'sa',
  password: 'sa123',
  server: `${os.hostname}\\SQLSERVER`,
  database: 'test',
  options: {
    encrypt: false,
  },
};

let pool; // Define the pool variable


function mapThingKeyToEquipment(thingKey) {
  const mappings = {
    '2cdc143f43': 'banbury6',
    '8a921t3t2d': 'banbury7',
    '52ac146b26': 'banbury8', // Added mapping
    '47799b3af4': 'banbury9', // Added mapping
    'bffcdd11ef': 'banbury10', // Added mapping
    '4tfa3b765b': 'Bartel Bead Winding',
    '5c58d23t67': 'Body ply cutter 1',
    'ee9e4a62ea': 'Body ply cutter 2',
    '18ac35cfde': 'Component Extruder',
    'c25beddt44': 'EBR',
    'efeded6tbb': 'Fischer Belt Cutter 1',
    '9ed1e149a1': 'Gum Calender',
    'c3a54419fc': 'Hot Calender',
    '86f8ada652': 'Inner Liner',
    'f1d597831d': 'Konstructa Belt Cutter 2',
    '193193d481': 'Super Assembly',
    '794ecat31d': 'TDA-1',
    'a9t2bb36f7': 'TDA-2',
    '527443tbe1': 'Tread Extruder',
    'f5a9551d7a': 'Vipo Bead Winding',
    // Add more mappings here
  };

  return mappings[thingKey] || '';
}

function handleServerError(error) {
  console.error('An error occurred:', error);

  if (error.code === 'ECONNRESET') {
    console.error('ECONNRESET error occurred. Restarting the server...');
    // Gracefully exit the current process, which will trigger an automatic restart if using a process manager
    // process.exit(1);
    // fetchEventDataWithRetry(pageNumber, thingKey[0]);
    // processThingKeys(0);
    processAllThingKeys()
    
    
  } else {
    console.error('Other error occurred. Restarting the server...');
    // Gracefully exit the current process
    // process.exit(1);
    // fetchEventDataWithRetry(pageNumber, thingKey)[0];
    // processThingKeys(0);
    processAllThingKeys()
  }
}

async function checkIfRecordExists(timestamp, thingKey) {
  const query = `
    SELECT COUNT(*) AS count
    FROM new_things_data
    WHERE timestamp = '${timestamp}'
    AND things_key = '${thingKey}'
  `;

  try {
    const result = await pool.request().query(query);
    const recordCount = result.recordset[0].count;
    console.log('Record count:', recordCount); // For debugging purposes
    return recordCount > 0;
  } catch (error) {
    console.error('Error checking record existence:', error);
    return true; // Return true to avoid insertion in case of error
  }
}

async function checkAndDeleteOneDuplicate(timestamp, thingKey) {
  const query = `
    SELECT id
    FROM new_things_data
    WHERE timestamp = '${timestamp}'
    AND things_key = '${thingKey}'
    ORDER BY id DESC
  `;

  try {
    const result = await pool.request().query(query);
    if (result.recordset.length != 0) {
      if(result.recordset.length > 1){
      const recordToDelete = result.recordset[0].id; // Get the latest duplicate
      console.log('Deleting duplicate record with id:', recordToDelete);
      
      const deleteQuery = `
        DELETE FROM new_things_data
        WHERE id = ${recordToDelete}
      `;
      
      await pool.request().query(deleteQuery);
      // console.log('Duplicate record deleted.');
      return true; // Indicate that a duplicate was deleted}
      }
      else{
        return true;
      }
    }

    return false; // No duplicates found
  } catch (error) {
    console.error('Error checking and deleting duplicate:', error);
    return true; // Return true to avoid insertion in case of error
  }
}


// Function to get the count of records for a specific thingKey for today
async function getDatabaseCountForThingKey(thingKey) {
  const currentDate = new Date().toISOString().split('T')[0]; // Get the current date in "YYYY-MM-DD" format

  // SQL query to count records for the specified thingKey for today
  const sql = `
    SELECT COUNT(*) AS count
    FROM events
    WHERE thing_key = ${thingKey} AND DATE(timestamp) = ${currentDate};
  `;

  // Execute the SQL query
  const result = await pool.request().query(sql);
  console.log('Record count:', results[0].count); // For debugging purposes
    const count = results[0].count;
   return count;
 
}


// Function to process thingKeys sequentially
// async function processThingKeys() {
//   // for (let i = 0; i < thingKeys.length; i++) {
//   //   const thingKey = thingKeys[i];
//   //   currentPage = 1; // Reset currentPage for each thingKey
//   //   await fetchEventDataWithRetry(currentPage, thingKey);
//   // }

//     await thingKeys.forEach((thingKey) => {
//       errorCountMap.set(thingKey, 0);
//        fetchEventDataWithRetry(currentPage, thingKey);
//     });

//   // // All thingKeys are processed, you can choose to restart or exit here
//   // console.log('All thingKeys processed. Restarting...');

//   // // Restart by calling processThingKeys again
//   // processThingKeys();
// }


// async function processThingKeys(index) {
//   const thingKey = thingKeys[index];
//   currentPage = 1; // Reset currentPage for each thingKey
//   await fetchEventDataWithRetry(currentPage, thingKey);

//   if (index === thingKeys.length - 1) {
//     // All thingKeys are processed, restart the process
//     console.log('All thingKeys processed. Restarting...');
//     processThingKeys(0); // Start from the beginning
//   } else {
//     // Process the next thingKey
//     processThingKeys(index + 1);
//   }
// }

async function processAllThingKeys() {
  const promises = thingKeys.map((thingKey) => {
    currentPage = 1; // Reset currentPage for each thingKey
    return fetchEventDataWithRetry(currentPage, thingKey);
  });

  await Promise.all(promises);

  console.log('All thingKeys processed. Restarting...');
  processAllThingKeys(); // Restart the process
}



async function insertEventData(eventData, thingKey) {
  let count = 0; // Counter for the number of records inserted

  

  for (const event of eventData) {
    const recordExists = await checkAndDeleteOneDuplicate(event.timestamp, thingKey);

      try {
    
        // console.log("records",recordExists)
        if (!recordExists) {
          const insertQuery = `
            INSERT INTO new_things_data (timestamp, created_at, things_key,
              Actual_Mixing_Energy, CARBON_1_CODE, CARBON_1_SET_WEIGHT, CARBON_1_ACTUAL_WEIGHT, CARBON_2_CODE, CARBON_2_SET_WEIGHT, CARBON_2_ACTUAL_WEIGHT,
              CARBON_3_CODE, CARBON_3_SET_WEIGHT, CARBON_3_ACTUAL_WEIGHT, carbonactualweight, CURRENT_CARBON_WEIGHT, dump_temperature, energykw_set,
              energykw_value, mixingactualtemp, mixingsettemp, mixingtime, OIL_1_CODE, OIL_1_ACTUAL_WEIGHT, OIL_1_SET_WEIGHT, OIL_2_CODE, OIL_2_ACTUAL_WEIGHT,
              OIL_2_SET_WEIGHT, OIL_3_CODE, OIL_3_ACTUAL_WEIGHT, OIL_3_SET_WEIGHT, POLYMER_1_CODE, POLYMER_1_ACTUAL_WEIGHT, POLYMER_1_SET_WEIGHT,
              POLYMER_2_CODE, POLYMER_2_ACTUAL_WEIGHT, POLYMER_2_SET_WEIGHT, POLYMER_3_CODE, POLYMER_3_ACTUAL_WEIGHT, POLYMER_3_SET_WEIGHT, POLYMER_4_CODE,
              POLYMER_4_ACTUAL_WEIGHT, POLYMER_4_SET_WEIGHT, POLYMER_5_CODE, POLYMER_5_ACTUAL_WEIGHT, POLYMER_5_SET_WEIGHT, TCU_1_SET, TCU_1_ACTUAL,
              TCU_2_SET, TCU_2_ACTUAL, TCU_3_SET, TCU_3_ACTUAL, Recipe_at_main_controller,Extruder_pressure_set, Extruder_pressure_actual, MATERIAL_WIDTH,INSPECTION_CHECK_WEIGHT_SET,INSPECTION_CHECK_WEIGHT_ACTUAL,WIDTH_1_DS_DRIVE_SIDE,WIDTH_1_OS_OPERATOR_SIDE,WIDTH_2_DS_DRIVE_SIDE,WIDTH_2_OS_OPERATOR_SIDE,MAIN_RECIPE,WINDUP_RECIPE,Irradiation_width_set_point,Material_width_set_point,Actual_dose,Cut_Width_actual_at_repair_conveyor,Cut_Width_setpoint_at_repair_conveyor,ROOM_TEMPERATURE,Width_set,Width_actual,Width_set_left,Width_actual_left,Width_set_right,Width_actual_right,Gauge_Drive_Side_Set,Gauge_Drive_Side_Actual,Gauge_Operator_Side_Set,Gauge_Operator_Side_Actual,Windup_Booking_Temperature,CREEL_ROOM_TEMPERATURE,CREEL_ROOM_HUMIDITY,Set_width,Actual_width,MATERIAL_OVERLAP_SET,INNERLINER_WIDTH_SET,INNERLINER_WIDTH_ACTUAL,SIDE_WALL_WIDTH_SET,RIGHT_SIDE_WALL_WIDTH_ACTUAL,LEFT_SIDE_WALL_WIDTH_ACTUAL,CONTINUOUS_SCALE_WEIGHT_SETPOINT,CONTINUOUS_SCALE_WEIGHT_ACTUAL,FINAL_WIDTH_SETPOINT,FINAL_WIDTH_ACTUAL,COOL_GUM_TEMPERATURE,CUSHION_WIDTH_ACTUAL,CUTTINGLENGTH_SET,SIMPLEX_WEIGHT_SET,SIMPLEX_WEIGHT_ACTUAL,SKIVER_recipe_set_cut_length,PRODUCT_WIDTH_SET,PRODUCT_WIDTH_ACTUAL,PRODUCT_CODE,CUTTING_WIDTH_SET,Equipment_Name)
            VALUES (@timestamp, @created_at, @things_key,
              @Actual_Mixing_Energy, @CARBON_1_CODE, @CARBON_1_SET_WEIGHT, @CARBON_1_ACTUAL_WEIGHT, @CARBON_2_CODE, @CARBON_2_SET_WEIGHT, @CARBON_2_ACTUAL_WEIGHT,
              @CARBON_3_CODE, @CARBON_3_SET_WEIGHT, @CARBON_3_ACTUAL_WEIGHT, @carbonactualweight, @CURRENT_CARBON_WEIGHT, @dump_temperature, @energykw_set,
              @energykw_value, @mixingactualtemp, @mixingsettemp, @mixingtime, @OIL_1_CODE, @OIL_1_ACTUAL_WEIGHT, @OIL_1_SET_WEIGHT, @OIL_2_CODE, @OIL_2_ACTUAL_WEIGHT,
              @OIL_2_SET_WEIGHT, @OIL_3_CODE, @OIL_3_ACTUAL_WEIGHT, @OIL_3_SET_WEIGHT, @POLYMER_1_CODE, @POLYMER_1_ACTUAL_WEIGHT, @POLYMER_1_SET_WEIGHT,
              @POLYMER_2_CODE, @POLYMER_2_ACTUAL_WEIGHT, @POLYMER_2_SET_WEIGHT, @POLYMER_3_CODE, @POLYMER_3_ACTUAL_WEIGHT, @POLYMER_3_SET_WEIGHT, @POLYMER_4_CODE,
              @POLYMER_4_ACTUAL_WEIGHT, @POLYMER_4_SET_WEIGHT, @POLYMER_5_CODE, @POLYMER_5_ACTUAL_WEIGHT, @POLYMER_5_SET_WEIGHT, @TCU_1_SET, @TCU_1_ACTUAL,
              @TCU_2_SET, @TCU_2_ACTUAL, @TCU_3_SET, @TCU_3_ACTUAL, @Recipe_at_main_controller, @Extruder_pressure_set, @Extruder_pressure_actual,@MATERIAL_WIDTH,@INSPECTION_CHECK_WEIGHT_SET,@INSPECTION_CHECK_WEIGHT_ACTUAL,@WIDTH_1_DS_DRIVE_SIDE,@WIDTH_1_OS_OPERATOR_SIDE,@WIDTH_2_DS_DRIVE_SIDE,@WIDTH_2_OS_OPERATOR_SIDE,@MAIN_RECIPE,@WINDUP_RECIPE,@Irradiation_width_set_point,@Material_width_set_point,@Actual_dose,@Cut_Width_actual_at_repair_conveyor,@Cut_Width_setpoint_at_repair_conveyor,@ROOM_TEMPERATURE,@Width_set,@Width_actual,@Width_set_left,@Width_actual_left,@Width_set_right,@Width_actual_right,@Gauge_Drive_Side_Set,@Gauge_Drive_Side_Actual,@Gauge_Operator_Side_Set,@Gauge_Operator_Side_Actual,@Windup_Booking_Temperature,@CREEL_ROOM_TEMPERATURE,@CREEL_ROOM_HUMIDITY,@Set_width,@Actual_width,@MATERIAL_OVERLAP_SET,@INNERLINER_WIDTH_SET,@INNERLINER_WIDTH_ACTUAL,@SIDE_WALL_WIDTH_SET,@RIGHT_SIDE_WALL_WIDTH_ACTUAL,@LEFT_SIDE_WALL_WIDTH_ACTUAL,@CONTINUOUS_SCALE_WEIGHT_SETPOINT,@CONTINUOUS_SCALE_WEIGHT_ACTUAL,@FINAL_WIDTH_SETPOINT,@FINAL_WIDTH_ACTUAL,@COOL_GUM_TEMPERATURE,@CUSHION_WIDTH_ACTUAL,@CUTTINGLENGTH_SET,@SIMPLEX_WEIGHT_SET,@SIMPLEX_WEIGHT_ACTUAL,@SKIVER_recipe_set_cut_length,@PRODUCT_WIDTH_SET,@PRODUCT_WIDTH_ACTUAL,@PRODUCT_CODE,@CUTTING_WIDTH_SET,@Equipment_Name)
          `;


          // const insertQuery = `
          //   INSERT INTO things_data (timestamp, created_at, things_key, width_set, recipe_at_main_controller, width_actual_right, width_actual_left, cut_Width_actual_at_repair_conveyor, width_actual)
          //   VALUES (@timestamp, @created_at, @things_key, @width_set, @recipe_at_main_controller, @width_actual_right, @width_actual_left, @cut_Width_actual_at_repair_conveyor, @width_actual)
          // `;

            // Usage example
          const equipmentName = mapThingKeyToEquipment(thingKey);
          
          
          // const apiTimestamp = new Date(event.timestamp).toISOString();
          // const apiCreate = new Date(event.created_at).toISOString()

          const receivedTimestampUTC = new Date(event.timestamp).toISOString();
          const createdAtUTC = new Date(event.created_at).toISOString();


          const insertRequest = pool.request()

        

            .input('timestamp', sql.DateTimeOffset, receivedTimestampUTC)
            .input('created_at', sql.DateTimeOffset, createdAtUTC)
            .input('things_key', sql.VarChar, thingKey)
            .input('Equipment_Name', sql.VarChar, equipmentName)
            // .input('Actual_Mixing_Energy', sql.VarChar, event.data.Actual_Mixing_Energy || '0')
            .input('Actual_Mixing_Energy', sql.VarChar, String(event.data.Actual_Mixing_Energy) || 'NULL')

            // .input('CARBON_1_CODE', sql.Float, event.data.CARBON_1_CODE || 0)
            .input('CARBON_1_CODE', sql.Float, parseFloat(event.data.CARBON_1_CODE) || 0)

            .input('CARBON_1_SET_WEIGHT', sql.Float, event.data.CARBON_1_SET_WEIGHT || 0)
            .input('CARBON_1_ACTUAL_WEIGHT', sql.Float, event.data.CARBON_1_ACTUAL_WEIGHT || 0)

            .input('CARBON_2_CODE', sql.Float, parseFloat(event.data.CARBON_2_CODE) || 0)

            .input('CARBON_2_SET_WEIGHT', sql.Float, event.data.CARBON_2_SET_WEIGHT || 0)
            .input('CARBON_2_ACTUAL_WEIGHT', sql.Float, event.data.CARBON_2_ACTUAL_WEIGHT || 0)
            .input('CARBON_3_CODE', sql.Float, parseFloat(event.data.CARBON_3_CODE) || 0)
            .input('CARBON_3_SET_WEIGHT', sql.Float, event.data.CARBON_3_SET_WEIGHT || 0)
            .input('CARBON_3_ACTUAL_WEIGHT', sql.Float, event.data.CARBON_3_ACTUAL_WEIGHT || 0)
            // ... Existing code ...

            .input('carbonactualweight', sql.Float, event.data.carbonactualweight || 0)
            .input('CURRENT_CARBON_WEIGHT', sql.Float, event.data.CURRENT_CARBON_WEIGHT || 0)
            .input('dump_temperature', sql.Float, event.data.dump_temperature || 0)
            .input('energykw_set', sql.Float, event.data.energykw_set || 0)
            .input('energykw_value', sql.Float, event.data.energykw_value || 0)
            .input('mixingactualtemp', sql.Float, event.data.mixingactualtemp || 0)
            .input('mixingsettemp', sql.Float, event.data.mixingsettemp || 0)
            .input('mixingtime', sql.Float, event.data.mixingtime || 0)

            .input('OIL_1_CODE', sql.Float, parseFloat(event.data.OIL_1_CODE) || 0)
            .input('OIL_1_ACTUAL_WEIGHT', sql.Float, event.data.OIL_1_ACTUAL_WEIGHT || 0)
            .input('OIL_1_SET_WEIGHT', sql.Float, event.data.OIL_1_SET_WEIGHT || 0)
            .input('OIL_2_CODE', sql.Float, parseFloat(event.data.OIL_2_CODE) || 0)
            .input('OIL_2_ACTUAL_WEIGHT', sql.Float, event.data.OIL_2_ACTUAL_WEIGHT || 0)
            .input('OIL_2_SET_WEIGHT', sql.Float, event.data.OIL_2_SET_WEIGHT || 0)
            .input('OIL_3_CODE', sql.Float, parseFloat(event.data.OIL_3_CODE) || 0)
            .input('OIL_3_ACTUAL_WEIGHT', sql.Float, event.data.OIL_3_ACTUAL_WEIGHT || 0)
            .input('OIL_3_SET_WEIGHT', sql.Float, event.data.OIL_3_SET_WEIGHT || 0)
            .input('POLYMER_1_CODE', sql.VarChar, event.data.POLYMER_1_CODE || '')
            .input('POLYMER_1_ACTUAL_WEIGHT', sql.Float, event.data.POLYMER_1_ACTUAL_WEIGHT || 0)
            .input('POLYMER_1_SET_WEIGHT', sql.Float, event.data.POLYMER_1_SET_WEIGHT || 0)
            .input('POLYMER_2_CODE', sql.VarChar, event.data.POLYMER_2_CODE || '')
            .input('POLYMER_2_ACTUAL_WEIGHT', sql.Float, event.data.POLYMER_2_ACTUAL_WEIGHT || 0)
            .input('POLYMER_2_SET_WEIGHT', sql.Float, event.data.POLYMER_2_SET_WEIGHT || 0)
            .input('POLYMER_3_CODE', sql.VarChar, event.data.POLYMER_3_CODE || '')
            .input('POLYMER_3_ACTUAL_WEIGHT', sql.Float, event.data.POLYMER_3_ACTUAL_WEIGHT || 0)
            .input('POLYMER_3_SET_WEIGHT', sql.Float, event.data.POLYMER_3_SET_WEIGHT || 0)
            .input('POLYMER_4_CODE', sql.VarChar, event.data.POLYMER_4_CODE || '')
            .input('POLYMER_4_ACTUAL_WEIGHT', sql.Float, event.data.POLYMER_4_ACTUAL_WEIGHT || 0)
            .input('POLYMER_4_SET_WEIGHT', sql.Float, event.data.POLYMER_4_SET_WEIGHT || 0)
            .input('POLYMER_5_CODE', sql.VarChar, event.data.POLYMER_5_CODE || '')
            .input('POLYMER_5_ACTUAL_WEIGHT', sql.Float, event.data.POLYMER_5_ACTUAL_WEIGHT || 0)
            .input('POLYMER_5_SET_WEIGHT', sql.Float, event.data.POLYMER_5_SET_WEIGHT || 0)
            .input('TCU_1_SET', sql.Float, event.data.TCU_1_SET || 0)
            .input('TCU_1_ACTUAL', sql.Float, event.data.TCU_1_ACTUAL || 0)
            .input('TCU_2_SET', sql.Float, event.data.TCU_2_SET || 0)
            .input('TCU_2_ACTUAL', sql.Float, event.data.TCU_2_ACTUAL || 0)
            .input('TCU_3_SET', sql.Float, event.data.TCU_3_SET || 0)
            .input('TCU_3_ACTUAL', sql.Float, event.data.TCU_3_ACTUAL || 0)


            .input('Recipe_at_main_controller', sql.VarChar, event.data.Recipe_at_main_controller || '')
            .input('Extruder_pressure_set', sql.Float, parseFloat(event.data.Extruder_pressure_set) || 0)
            .input('Extruder_pressure_actual', sql.Float, parseFloat(event.data.Extruder_pressure_actual) || 0)

            .input('MATERIAL_WIDTH', sql.Float, parseFloat(event.data.MATERIAL_WIDTH) || 0)


            .input('INSPECTION_CHECK_WEIGHT_SET', sql.Float, parseFloat(event.data.INSPECTION_CHECK_WEIGHT_SET) || 0)
            .input('INSPECTION_CHECK_WEIGHT_ACTUAL', sql.Float, parseFloat(event.data.INSPECTION_CHECK_WEIGHT_ACTUAL) || 0)
            .input('WIDTH_1_DS_DRIVE_SIDE', sql.Float, parseFloat(event.data.WIDTH_1_DS_DRIVE_SIDE) || 0)
            .input('WIDTH_1_OS_OPERATOR_SIDE', sql.Float, parseFloat(event.data.WIDTH_1_OS_OPERATOR_SIDE) || 0)
            .input('WIDTH_2_DS_DRIVE_SIDE', sql.Float, parseFloat(event.data.WIDTH_2_DS_DRIVE_SIDE) || 0)
            .input('WIDTH_2_OS_OPERATOR_SIDE', sql.Float, parseFloat(event.data.WIDTH_2_OS_OPERATOR_SIDE) || 0)
            .input('MAIN_RECIPE', sql.VarChar, event.data.MAIN_RECIPE || '')
            .input('WINDUP_RECIPE', sql.VarChar, event.data.WINDUP_RECIPE || '')


            .input('Irradiation_width_set_point', sql.Float, parseFloat(event.data.Irradiation_width_set_point) || 0)
            .input('Material_width_set_point', sql.Float, parseFloat(event.data.Material_width_set_point) || 0)
            .input('Actual_dose', sql.Float, parseFloat(event.data.Actual_dose) || 0)


            .input('Cut_Width_actual_at_repair_conveyor', sql.Float, parseFloat(event.data.Cut_Width_actual_at_repair_conveyor) || 0)
            .input('Cut_Width_setpoint_at_repair_conveyor', sql.Float, parseFloat(event.data.Cut_Width_setpoint_at_repair_conveyor) || 0)
            .input('ROOM_TEMPERATURE', sql.Float, parseFloat(event.data.ROOM_TEMPERATURE) || 0)
            .input('Width_set', sql.Float, parseFloat(event.data.Width_set) || 0)
            .input('Width_actual', sql.Float, parseFloat(event.data.Width_actual) || 0)
            .input('Width_set_left', sql.Float, parseFloat(event.data.Width_set_left) || 0)
            .input('Width_actual_left', sql.Float, parseFloat(event.data.Width_actual_left) || 0)
            .input('Width_set_right', sql.Float, parseFloat(event.data.Width_set_right) || 0)
            .input('Width_actual_right', sql.Float, parseFloat(event.data.Width_actual_right) || 0)


          .input('Gauge_Drive_Side_Set', sql.Float, parseFloat(event.data.Gauge_Drive_Side_Set) || 0)
          .input('Gauge_Drive_Side_Actual', sql.Float, parseFloat(event.data.Gauge_Drive_Side_Actual) || 0)
          .input('Gauge_Operator_Side_Set', sql.Float, parseFloat(event.data.Gauge_Operator_Side_Set) || 0)
          .input('Gauge_Operator_Side_Actual', sql.Float, parseFloat(event.data.Gauge_Operator_Side_Actual) || 0)


           .input('Windup_Booking_Temperature', sql.Float, parseFloat(event.data.Windup_Booking_Temperature) || 0)
           .input('CREEL_ROOM_TEMPERATURE', sql.Float, parseFloat(event.data.CREEL_ROOM_TEMPERATURE) || 0)
           .input('CREEL_ROOM_HUMIDITY', sql.Float, parseFloat(event.data.CREEL_ROOM_HUMIDITY) || 0)


          // .input('PRODUCT_WIDTH_SET', sql.Float, event.data.PRODUCT_WIDTH_SET || 0)
          // .input('PRODUCT_WIDTH_ACTUAL', sql.Float, event.data.PRODUCT_WIDTH_ACTUAL || 0)
          // .input('PRODUCT_CODE', sql.Float, event.data.PRODUCT_CODE || 0)
          // .input('CUTTING_WIDTH_SET', sql.Float, event.data.CUTTING_WIDTH_SET || 0)

          .input('Set_width', sql.Float, parseFloat(event.data.Set_width) || 0)
          .input('Actual_width', sql.Float, parseFloat(event.data.Actual_width) || 0)
          .input('MATERIAL_OVERLAP_SET', sql.Float, parseFloat(event.data.MATERIAL_OVERLAP_SET) || 0)
          .input('INNERLINER_WIDTH_SET', sql.Float, parseFloat(event.data.INNERLINER_WIDTH_SET) || 0)
          .input('INNERLINER_WIDTH_ACTUAL', sql.Float, parseFloat(event.data.INNERLINER_WIDTH_ACTUAL) || 0)
          .input('SIDE_WALL_WIDTH_SET', sql.Float, parseFloat(event.data.SIDE_WALL_WIDTH_SET) || 0)
          .input('RIGHT_SIDE_WALL_WIDTH_ACTUAL', sql.Float, parseFloat(event.data.RIGHT_SIDE_WALL_WIDTH_ACTUAL) || 0)
          .input('LEFT_SIDE_WALL_WIDTH_ACTUAL', sql.Float, parseFloat(event.data.LEFT_SIDE_WALL_WIDTH_ACTUAL) || 0)

          .input('CONTINUOUS_SCALE_WEIGHT_SETPOINT', sql.Float, event.data.CONTINUOUS_SCALE_WEIGHT_SETPOINT || 0)
          .input('CONTINUOUS_SCALE_WEIGHT_ACTUAL', sql.Float, event.data.CONTINUOUS_SCALE_WEIGHT_ACTUAL || 0)
          .input('FINAL_WIDTH_SETPOINT', sql.Float, event.data.FINAL_WIDTH_SETPOINT || 0)
          .input('FINAL_WIDTH_ACTUAL', sql.Float, event.data.FINAL_WIDTH_ACTUAL || 0)
          .input('COOL_GUM_TEMPERATURE', sql.Float, event.data.COOL_GUM_TEMPERATURE || 0)

          .input('CUSHION_WIDTH_ACTUAL', sql.Float,  parseFloat(event.data.CUSHION_WIDTH_ACTUAL) || 0)
          .input('CUTTINGLENGTH_SET', sql.Float,  parseFloat(event.data.CUTTINGLENGTH_SET) || 0)
          .input('SIMPLEX_WEIGHT_SET', sql.Float,  parseFloat(event.data.SIMPLEX_WEIGHT_SET) || 0)
          .input('SIMPLEX_WEIGHT_ACTUAL', sql.Float,  parseFloat(event.data.SIMPLEX_WEIGHT_ACTUAL) || 0)
          .input('SKIVER_recipe_set_cut_length', sql.Float,  parseFloat(event.data.SKIVER_recipe_set_cut_length) || 0)

          .input('PRODUCT_WIDTH_SET', sql.Float,  parseFloat(event.data.PRODUCT_WIDTH_SET) || 0)
          .input('PRODUCT_WIDTH_ACTUAL', sql.Float,  parseFloat(event.data.PRODUCT_WIDTH_ACTUAL) || 0)
          .input('PRODUCT_CODE', sql.Float,  parseFloat(event.data.PRODUCT_CODE) || 0)
          .input('CUTTING_WIDTH_SET', sql.Float,  parseFloat(event.data.CUTTING_WIDTH_SET) || 0)


          insertRequest.query(insertQuery)
            .then(() => {
              count++;
              console.log(`Record ${count}/${eventData.length} inserted successfully for page ${currentPage} of thing key ${thingKey}`);
            })
            .catch((error) => {
              handleServerError(error);
              console.error('Error inserting data:', error);
            });
        } else {
          console.log('Record already exists: for:',thingKey,  event.timestamp, "On", new Date().toLocaleString('en-US', { timeZone: 'Asia/Calcutta' }));
        }
    
    }
    catch (error) {
      handleServerError(error);
      console.error('An error occurred during the database query:', error);
    }
  }
}





// Define a GET endpoint
app.get('/retry-count', (req, res) => {
  res.json({ retryCounts });
});




const retryDelay = 2000; // Retry after 5 seconds

// Define a map to store the error count for each thingKey
const errorCountMap = new Map();

// Function to fetch event data with retry mechanism
async function fetchEventDataWithRetry(pageNumber, thingKey) {
  const currentDate = new Date().toISOString().split('T')[0]; // Get the current date in "YYYY-MM-DD" format

  const data = {
    idle_time_required: true,
    time_zone: 'Asia/Calcutta',
    thing_key: thingKey,
    from: `${currentDate} 00:00:00`,
    to: `${currentDate} 23:59:59`,
    page: pageNumber,
    page_size: pageSize
  };

  const axiosInstance = axios.create({
    baseURL: apiUrl,
    headers,
    httpsAgent: new https.Agent({
      rejectUnauthorized: false, // Disable SSL certificate validation
    }),
    timeout: 50000,
  });
  return new Promise(async (resolve) => {
  try {
    // const response = await axios.post(apiUrl, data, { headers });
    const response = await axiosInstance.post(apiUrl, data);
    const apiResponse = response.data;

    if (!apiResponse[thingKey] || !apiResponse[thingKey].event_data) {
      console.error('Data not available or incorrect:', apiResponse);
      return;
    }

    // Reset error count when successful response is received
    errorCountMap.set(thingKey, 0);

    const eventData = apiResponse[thingKey].event_data;
    totalRecords = apiResponse[thingKey].total_event_count;
    totalPages = Math.ceil(totalRecords / pageSize);

    // const databaseCount = getDatabaseCountForThingKey(thingKey); // Implement this function
    // if (totalRecords !== databaseCount) {
    //   insertEventData(eventData, thingKey);
    // }

    insertEventData(eventData, thingKey);
    // Return a promise that resolves when the processing is complete
 
  

    if (currentPage < totalPages) {
      currentPage++;
      await fetchEventDataWithRetry(currentPage, thingKey);
    } else {
      // If all pages for this thingKey are fetched, move to the next thingKey
      // const currentIndex = thingKeys.indexOf(thingKey);
      // if (currentIndex < thingKeys.length - 1) {
      //   const nextThingKey = thingKeys[currentIndex + 1];
      //   console.log("nextThingsKey",nextThingKey)
      //   currentPage = 1; // Reset currentPage for the next thingKey
      //   fetchEventDataWithRetry(currentPage, nextThingKey);
      // } else {
      //   // All thingKeys are processed, you can choose to restart or exit here
      //   console.log('All thingKeys processed. Restarting...');
      //   // Restart by calling fetchEventDataWithRetry with the first thingKey again
      //   currentPage = 1;
      //   fetchEventDataWithRetry(currentPage, thingKeys[0]);
      // }
      resolve();
      processAllThingKeys();
    }



  } catch (error) {

    handleServerError(error);
    console.error('An error occurred:', error);

    if (error.code === 'ECONNRESET') {
      // Increment error count for this thingKey
      const currentErrorCount = errorCountMap.get(thingKey) || 0;
      errorCountMap.set(thingKey, currentErrorCount + 1);

      console.error('ECONNRESET error occurred. Retrying after delay...');
      // fetchEventDataWithRetry(pageNumber, thingKey);
      processAllThingKeys();
    } else {
      // fetchEventDataWithRetry(pageNumber, thingKey);
      processAllThingKeys();
    }
    
     // Increment the error count for this thingKey
     const currentErrorCount = errorCountMap.get(thingKey) || 0;
     errorCountMap.set(thingKey, currentErrorCount + 1);
  }
});
}



// Connect to the MSSQL database
sql
  .connect(config)
  .then((poolInstance) => {
    console.log('Connected to the MSSQL database.');
    pool = poolInstance; // Assign the pool instance to the global variable
  
    // Call the fetchEventData function for each thingKey to start fetching and inserting the data
    // thingKeys.forEach((thingKey) => {
    //   errorCountMap.set(thingKey, 0);
    //   fetchEventDataWithRetry(currentPage, thingKey);
    // });

    // fetchEventDataWithRetry(currentPage, thingKeys[0]);
    processAllThingKeys();

  })
  .catch((err) => {
    console.error('Error:', err);
  });


  app.get('/fetch_data', (req, res) => {
    // thingKeys.forEach((thingKey) => {
    //   errorCountMap.set(thingKey, 0);
    //   fetchEventDataWithRetry(currentPage, thingKey);
    // });
    processAllThingKeys();
    res.send('Fetching data...');
  });


  // Define a GET endpoint
  app.get('/', (req, res) => {
    const errorCounts = {};
    for (const thingKey of thingKeys) {
      errorCounts[thingKey] = errorCountMap.get(thingKey) || 0;
    }
    
    res.json({
      message: `JK Tyres  API response at port ${PORT}`,
      errorCounts: errorCounts
    });
  });


// Middleware to handle errors
  app.use((err, req, res, next) => {
    console.error(err.stack);
    res.status(500).send('Something broke!');
  });

  

// Start the server

const server = http.createServer(app);

server.on('error', (error) => {
  console.error('Server error:', error);
});

server.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});





// Handle unhandled exceptions
process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err);
  restartServer();
});

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection:', reason);
  restartServer();
});

function restartServer() {
  console.log('Restarting the server...');
  
  // Close the server gracefully
  server.close((err) => {
    if (err) {
      console.error('Error closing server:', err);
      processAllThingKeys();
    } else {
      console.log('Server closed gracefully.');
      
      // Start the server again
      server.listen(PORT, () => {
        console.log(`Server is running on port ${PORT} again.`);
        // fetchEventDataWithRetry(currentPage, thingKeys[0]);
        processAllThingKeys();
      });
    }
  });
}


// app.listen(PORT, () => {
//   console.log(`Server is running on port ${PORT}`);
// });


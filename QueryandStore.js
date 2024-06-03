const oracledb = require('oracledb');
const fs = require('fs');
const path = require('path');
const os = require('os');

console.log('Starting QueryandStore.js script.');

async function readFileContents(filePath) {
  try {
    const fileContents = fs.readFileSync(filePath, 'utf8');
    const lines = fileContents.split('\n');
    console.log(`File read successfully: ${filePath}`);
    return { connectionString: lines[0].trim(), query: lines.slice(1).join('\n').trim() };
  } catch (err) {
    console.error(`Failed to read file ${filePath}: ${err.message}`);
    throw err; 
  }
}

function parseConnectionString(connectionString) {
  try {
    const credentials = {
      user: '',
      password: '',
      remainingConnectionString: ''
    };
  
    const userIdMatch = connectionString.match(/User Id=([^;]*);/i);
    const passwordMatch = connectionString.match(/Password=([^;]*);/i);
  
    credentials.user = userIdMatch ? userIdMatch[1].trim() : '';
    credentials.password = passwordMatch ? passwordMatch[1].trim() : '';
    credentials.remainingConnectionString = connectionString
      .replace(/User Id=[^;]*;/i, '')
      .replace(/Password=[^;]*;/i, '')
      .replace(/Data Source=/i, '')
      .trim();
  
    console.log(`Connection string parsed successfully for user: ${credentials.user}`);
    return credentials;
  } catch (err) {
    console.error(`Failed to parse connection string: ${err.message}`);
    throw err;
  }
}

function toCSV(data) {
  try {
    const header = 'barcodes,carrierid,timestamp';
    const csvRows = data.map(row =>
      row.map(field =>
        `"${String(field).replace(/"/g, '""')}"`
      ).join(',')
    ).join(os.EOL);
    console.log(`Data converted to CSV format successfully`);
    return `${header}${os.EOL}${csvRows}`;
  } catch (err) {
    console.error(`Error converting data to CSV: ${err.message}`);
    throw err;
  }
}

function prepareStoragePath(basePath, subdir) {
  try {
    const now = new Date();
    const dateNow = now.toISOString().split('T')[0];
    const storagePath = path.join(basePath, subdir + dateNow);
    if (!fs.existsSync(storagePath)) {
      fs.mkdirSync(storagePath, { recursive: true }); 
      console.log(`Storage directory created: ${storagePath}`);
    }
    return storagePath;
  } catch (err) {
    console.error(`Error creating storage path ${subdir}: ${err.message}`);
    throw err;
  }
}

function writeResultToFile(csvData, filePath) {
  try {
    const now = new Date();
    const time = now.toISOString().substring(11, 19).replace(/:/g, '-');
    const baseFileName = path.basename(filePath, '.sql');
    const outputFileName = `${baseFileName}_${time}.csv`;
    const outputPath = path.join(filePath, outputFileName);
    fs.writeFileSync(outputPath, csvData);
    console.log(`Result written to CSV file ${outputPath}`);
  } catch (err) {
    console.error(`Failed to write result to file: ${err.message}`);
    throw err;
  }
}

async function mainThread(filePath) {
  
  let connection;
  try {
    const { connectionString, query } = await readFileContents(filePath);
    const { user, password, remainingConnectionString } = parseConnectionString(connectionString);
    connection = await oracledb.getConnection({ user, password, connectionString: remainingConnectionString });
    const result = await connection.execute(query);
    console.log(`Query executed successfully.`);
    const csvData = toCSV(result.rows);
    const storagePath = prepareStoragePath(__dirname, 'StorageResults/');
    writeResultToFile(csvData, storagePath);

  } catch (err) {
    console.error(`Error while processing file ${filePath}:`, err);
  } finally {
    if (connection) {
      try {
        await connection.close();
        console.log(`Connection closed for file.`);
      } catch (err) {
        console.error(`Error closing the connection for file ${filePath}:`, err);
      }
    }
  }
}

async function runQueriesInFolder(folderPath) {
  console.log(`Processing SQL files in folder: ${folderPath}`);
  const files = fs.readdirSync(folderPath);
  const queryPromises = files.filter(file => path.extname(file) === '.sql').map(file => mainThread(path.join(folderPath, file)));
  await Promise.all(queryPromises);
  const { exec } = require('child_process');
  console.log(`running S3 uploader...`);
  exec(`python3 s3_uploader.py`, (error, stdout, stderr) => {
    if (error) {
      console.error(`Error running S3 uploader script: ${error.message}`);
      return;
    }
    if (stderr) {
      console.error(`stderr from S3 uploader: ${stderr}`);
      return;
    }
    console.log(`stdout from S3 uploader: ${stdout}`);
  });
  console.log('Script execution complete.');
}

runQueriesInFolder(process.cwd());


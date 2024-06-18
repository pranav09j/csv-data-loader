import express from 'express'
import fs from 'fs'
import { upload } from './middleware/multer.middleware.js';

const app = express();

import {insertFromCSV, updateFromCSV, deleteFromCSV, readFromMongoDB} from './controller/csv.controller.js'




app.post('/insert-csv', upload.single('file'), async (req, res) => {

  const csvFilePath = req.file.path;
  await insertFromCSV(csvFilePath);
  res.send('CSV data loaded into MongoDB');
  fs.unlinkSync(req.file.path); // Remove file after processing
});

app.get('/read-all', async (req, res) => {
  
  const data = await readFromMongoDB();
  res.json(data);
  console.log('Data from MongoDB:', data);
});

app.post('/update-csv', upload.single('file'), async (req, res) => {
 
  const csvFilePath = req.file.path;
  await updateFromCSV(csvFilePath);
  res.send('Documents updated from CSV');
  // fs.unlinkSync(req.file.path); // Remove file after processing
  
});

app.post('/delete-csv', upload.single('file'), async (req, res) => {
   
    const csvFilePath = req.file.path;
    await deleteFromCSV(csvFilePath);
    res.send('Documents deleted from CSV');
    
});

const port = 3000;


app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
});



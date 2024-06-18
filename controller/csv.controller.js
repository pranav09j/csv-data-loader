
import dotenv from 'dotenv'
import fs from 'fs'
import csvParser from 'csv-parser'
import { MongoClient, ObjectId } from 'mongodb';
import {DB_NAME } from '../constants.js';

dotenv.config({
    path: './.env'
})

const mongoURI = process.env.MONGO_URI;
const dbName = `${DB_NAME}`; 
const collectionName = process.env.COLLECTION_NAME;

// Function to connect to MongoDB
async function connectToMongoDB() {
  const client = new MongoClient(mongoURI, { useNewUrlParser: true, useUnifiedTopology: true });
  try {
    await client.connect();
    console.log('Connected to MongoDB');
    const db = client.db(dbName);
    const collection = db.collection(collectionName);
    return collection;
  } catch (err) {
    console.error('Error connecting to MongoDB:', err);
    throw err;
  }
}


// Function to insert data from CSV into MongoDB
async function insertFromCSV(csvFilePath) {
  try {
    const collection = await connectToMongoDB();
    const results = [];
    fs.createReadStream(csvFilePath)
      .pipe(csvParser())
      .on('data', (data) => results.push(data))
      .on('end', async () => {
        const insertResult = await collection.insertMany(results);
        console.log(`${insertResult.insertedCount} documents inserted`);
      });
  } catch (err) {
    console.error('Error inserting from CSV:', err);
  }
}

// Function to update data in MongoDB from CSV
async function updateFromCSV(csvFilePath) {
  try {
    const collection = await connectToMongoDB();
    const results = [];
    fs.createReadStream(csvFilePath)
      .pipe(csvParser())
      .on('data', (data) => results.push(data))
      .on('end', async () => {
        for (const data of results) {
          const { _id, ...updateData } = data;
          await collection.updateOne({ _id:new ObjectId(_id) }, { $set: updateData });
        }
        console.log('Update operation complete');
      });
  } catch (err) {
    console.error('Error updating from CSV:', err);
  }
}

// Function to delete data in MongoDB based on CSV
async function deleteFromCSV(csvFilePath) {
  try {
    const collection = await connectToMongoDB();
    const results = [];
    fs.createReadStream(csvFilePath)
      .pipe(csvParser())
      .on('data', (data) => results.push(data))
      .on('end', async () => {
        for (const data of results) {
          const { _id } = data;
          await collection.deleteOne({ _id:new ObjectId(_id) });
        }
        console.log('Delete operation complete');
      });
  } catch (err) {
    console.error('Error deleting from CSV:', err);
  }
}

// Function to read data from MongoDB
async function readFromMongoDB(query = {}) {
  try {
    const collection = await connectToMongoDB();
    const cursor = collection.find(query);
    const results = await cursor.toArray();
    return results;
  } catch (err) {
    console.error('Error reading from MongoDB:', err);
    throw err;
  }
}

export {
  insertFromCSV,
  updateFromCSV,
  deleteFromCSV,
  readFromMongoDB
};

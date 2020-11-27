/** @format */

const MongoCollection = require('./MongoCollection')
const Random = require('./Random')
const connectToDatabase = require('./connect')
const loadCollections = require('./loadCollections')

module.exports = {
  MongoCollection,
  connectToDatabase,
  loadCollections,
  Random
}

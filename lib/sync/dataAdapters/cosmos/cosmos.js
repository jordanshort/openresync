const _ = require('lodash')
const fsPromises = require('fs').promises
const moment = require('moment')
const { getPrimaryKeyField, getTimestampFields, shouldIncludeField } = require('../../utils')
const { getIndexes } = require('../../indexes')
const { Worker } = require('worker_threads')
const pathLib = require('path')
const CosmosClient = require('@azure/cosmos').CosmosClient;

module.exports = ({ destinationConfig }) => {
  let platformAdapter
  let platformDataAdapter
  const endpointUri = destinationConfig.endpointUri;
  const key = destinationConfig.key;
  const databaseId = destinationConfig.databaseId;

  const client = new CosmosClient({ endpoint: endpointUri, key })
  const database = client.database(databaseId)

  const userMakeTableName = destinationConfig.makeTableName
  const userMakeFieldName = destinationConfig.makeFieldName
  const userMakeForeignKeyFieldName = destinationConfig.makeForeignKeyFieldName
  const userTransform = destinationConfig.transform
  const userShouldSyncTableSchema = destinationConfig.shouldSyncTableSchema

  function setPlatformAdapter(adapter) {
    platformAdapter = adapter
  }

  function setPlatformDataAdapter(adapter) {
    platformDataAdapter = adapter
  }

  async function maybeDropOrTruncateTable(mlsResourceObj) {
    // not implemented
  }

  async function createIfNotExists(containerId, partitionKey) {
    /**
     * Create the database if it does not exist
     */
    // const { database } = await client.databases.createIfNotExists({
    //   id: databaseId
    // });
  
    /**
     * Create the container if it does not exist
     */
    const { container } = await client
      .database(databaseId)
      .containers.createIfNotExists(
        { id: containerId, partitionKey },
        { offerThroughput: 1000 }
      );
  
    return container;
  }

  async function syncStructure(mlsResourceObj) {
    // For debug convenience.
    await maybeDropOrTruncateTable(mlsResourceObj)
    if (mlsResourceObj.expand) {
      for (const subMlsResourceObj of mlsResourceObj.expand) {
        await maybeDropOrTruncateTable(subMlsResourceObj)
      }
    }
  }

  function makeName(name) {
    return name
  }

  function makeContainerId(name) {
    return userMakeTableName ? userMakeTableName(name) : makeName(name)
  }

  function makeFieldName(mlsResourceName, name) {
    return userMakeFieldName ? userMakeFieldName(mlsResourceName, name) : makeName(name)
  }

  function transform(mlsResourceName, record, metadata) {
    return userTransform ? userTransform(mlsResourceName, record, metadata) : record
  }

  async function syncData(mlsResourceObj, mlsData, metadata) {
    if (!mlsData.length) {
      return
    }
    for (const d of mlsData) {
      for (const key in d) {
        if (key.endsWith('Timestamp') && d[key]) {
          d[key] = moment.utc(d[key]).format("YYYY-MM-DD HH:mm:ss.SSS")
        } else if (key.endsWith('YN')) {
          d[key] = d[key] ? 1 : 0
        }
      }
    }

    const containerId = makeContainerId(mlsResourceObj.name)
    const indexes = getIndexes(mlsResourceObj.name)
    const partitionKey = getPartitionKey(mlsResourceObj.name)
    await createIfNotExists(containerId, partitionKey)

    const container = database.container(containerId)
    let fieldNames = Object.keys(mlsData[0])
      .filter(fieldName => shouldIncludeField(fieldName, indexes, platformAdapter.shouldIncludeJsonField, mlsResourceObj.select))
    const transformedMlsData = mlsData.map(x => {
      const val = _.pick(x, fieldNames)
      const partitionKeyVal = val[getPrimaryKeyField(mlsResourceObj.name, indexes)];
      val.id = partitionKeyVal;
      return transform(mlsResourceObj.name, val, metadata);
    })

    for (let i = 0; i < transformedMlsData.length; i++) {
      await container.items.upsert(transformedMlsData[i]);
    }
  }

  async function getTimestamps(mlsResourceName, indexes) {
    const tableName = makeContainerId(mlsResourceName)
    const updateTimestampFields = _.pickBy(indexes, v => v.isUpdateTimestamp)
    const fieldsString = _.map(updateTimestampFields, (v, k) => `MAX(${tableName}.${makeFieldName(mlsResourceName, k)}) as ${k}`).join(', ')
    const partitionKey = getPartitionKey(mlsResourceName)
    const container = await createIfNotExists(tableName, partitionKey);
    const querySpec = {
      query: `SELECT ${fieldsString} FROM ${tableName}`
    }
    const { resources: items } = await container.items.query(querySpec).fetchAll();
    // DEBUG
    // return _.mapValues(updateTimestampFields, (v, k) => items[0][k] || new Date(new Date().setFullYear(new Date().getFullYear() - 1)))
    return _.mapValues(updateTimestampFields, (v, k) => items[0][k] || new Date(0))
  }

  async function getAllIds(mlsResourceName, indexes) {
    const tableName = makeContainerId(mlsResourceName)
    const officialFieldName = getPrimaryKeyField(mlsResourceName, indexes)
    const userFieldName = makeFieldName(mlsResourceName, officialFieldName)
    const partitionKey = getPartitionKey(mlsResourceName)
    const container = await createIfNotExists(tableName, partitionKey);
    const querySpec = {
      query: `SELECT ${tableName}.${userFieldName} AS id FROM ${tableName} ORDER BY ${tableName}.${userFieldName}`
    }
    const { resources: items } = await container.items.query(querySpec).fetchAll();
    const ids = items.map(x => x.id)
    // good for debugging
    // return ids.slice(0, 200);
    return ids
  }

  async function getCount(mlsResourceName) {
    const tableName = makeContainerId(mlsResourceName)
    const partitionKey = getPartitionKey(mlsResourceName)
    const container = await createIfNotExists(tableName, partitionKey);

    const sql = `SELECT COUNT(1) as count FROM ${tableName}`
    const { resources: items } = await container.items.query({ query: sql }).fetchAll();
    return items[0].count
  }

  async function getMostRecentTimestamp(mlsResourceName) {
    const tableName = makeContainerId(mlsResourceName)
    const partitionKey = getPartitionKey(mlsResourceName)
    const container = await createIfNotExists(tableName, partitionKey);

    const sql = `SELECT MAX(${tableName}.ModificationTimestamp) as val FROM ${tableName}`
    const { resources: items } = await container.items.query({ query: sql }).fetchAll();
    if (!items.length) {
      return null
    }
    return items[0].val
  }

  function getPartitionKey(mlsResourceName) {
    const indexes = getIndexes(mlsResourceName);
    return `/${getPrimaryKeyField(mlsResourceName, indexes)}`;
  }

  async function purge(mlsResourceObj, idsToPurge) {
    const mlsResourceName = mlsResourceObj.name
    const tableName = makeContainerId(mlsResourceName)
    const container = await client
      .database(databaseId).container(tableName);
    for (let i = 0; i < idsToPurge.length; i++) {
      const id = idsToPurge[i];
      await container.item(id, id).delete();
    }
  }

  async function closeConnection() {
    // return db.destroy()
  }

  function dotSeparator(term, tableName) {
    return tableName + '.' + term
  }

  // "Missing IDs data" means that the goal is to understand which records are not up to date in a reconcile process.
  // So to do that, we look at fields like ModificationTimestamp, PhotosChangeTimestamp, etc. It's those multiple fields
  // that we look at that I'm calling the "data".
  async function fetchMissingIdsData(mlsResourceName, indexes) {
    const tableName = makeContainerId(mlsResourceName)
    const officialFieldName = getPrimaryKeyField(mlsResourceName, indexes)
    const userFieldName = makeFieldName(mlsResourceName, officialFieldName)
    const timestampFieldNames = getTimestampFields(mlsResourceName, indexes)
    const cosmosTimestampFieldNames = timestampFieldNames.map(x => makeFieldName(mlsResourceName, x))
    const fieldNamesSql = [userFieldName, ...cosmosTimestampFieldNames]
      .map((term) => dotSeparator(term, tableName))
      .join(', ')
    const sql = `SELECT ${fieldNamesSql} FROM ${tableName} ORDER BY ${tableName}.${userFieldName}`
    const partitionKey = getPartitionKey(mlsResourceName)
    const container = await createIfNotExists(tableName, partitionKey);
    const { resources: items } = await container.items.query({ query: sql }).fetchAll();
    return items;
  }

  function computeMissingIds(mlsResourceName, dataInMls, dataInAdapter, indexes) {
    const officialFieldName = getPrimaryKeyField(mlsResourceName, indexes)
    const userFieldName = makeFieldName(mlsResourceName, officialFieldName)
    const timestampFieldNames = getTimestampFields(mlsResourceName, indexes)
    const mysqlTimestampFieldNames = timestampFieldNames.map(x => makeFieldName(mlsResourceName, x))
    const workerPath = pathLib.resolve(__dirname, 'worker.js')
    return new Promise((resolve, reject) => {
      const worker = new Worker(workerPath, {
        workerData: {
          dataInAdapter,
          userFieldName,
          dataInMls,
          officialFieldName,
          timestampFieldNames,
          mysqlTimestampFieldNames,
        },
      })
      worker.on('message', missingOrOldIds => {
        resolve(missingOrOldIds)
      })
      worker.on('error', error => {
        reject(error)
      })
    })
  }

  return {
    syncStructure,
    syncData,
    getTimestamps,
    closeConnection,
    setPlatformAdapter,
    setPlatformDataAdapter,
    getAllIds,
    purge,
    getCount,
    getMostRecentTimestamp,
    fetchMissingIdsData,
    computeMissingIds,
  }
}

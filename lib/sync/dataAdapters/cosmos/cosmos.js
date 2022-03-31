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
  // const { connectionString, endpointUri, key, databaseId } = destinationConfig;
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
    // await db.raw(`drop table if exists \`${makeTableName(mlsResourceObj.name)}\``)
    // await db.raw(`truncate \`${makeTableName(mlsResourceObj.name)}\``)
  }

  async function createIfNotExists(containerId, partitionKey) {
    /**
     * Create the database if it does not exist
     */
    // const { database } = await client.databases.createIfNotExists({
    //   id: databaseId
    // });
    // console.log(`Created database:\n${database.id}\n`);
  
    /**
     * Create the container if it does not exist
     */
    const { container } = await client
      .database(databaseId)
      .containers.createIfNotExists(
        { id: containerId, partitionKey },
        { offerThroughput: 1000 }
      );
  
    console.log(`Created container:\n${container.id}\n`);
    return container;
  }

  async function syncStructure(mlsResourceObj, metadata) {
    // This is how we get around the fact that we have 600+ columns and the row size is greater than what's allowed.
    // await db.raw('SET SESSION innodb_strict_mode=OFF')

    // For debug convenience.
    await maybeDropOrTruncateTable(mlsResourceObj)
    if (mlsResourceObj.expand) {
      for (const subMlsResourceObj of mlsResourceObj.expand) {
        await maybeDropOrTruncateTable(subMlsResourceObj)
      }
    }

    // const schemas = metadata['edmx:Edmx']['edmx:DataServices'][0].Schema
    // const entityTypes = platformAdapter.getEntityTypes(schemas)
    // const [rows] = await db.raw(`SELECT table_name AS tableName FROM information_schema.tables WHERE table_schema = DATABASE()`)
    // const tableNames = rows.map(x => x.tableName)
    // if (shouldSyncTableSchema(mlsResourceObj.name)) {
    //   await effectTable(mlsResourceObj, tableNames, entityTypes)
    // }
    // if (mlsResourceObj.expand) {
    //   for (const subMlsResourceObj of mlsResourceObj.expand) {
    //     if (shouldSyncTableSchema(subMlsResourceObj.name)) {
    //       await effectTable(subMlsResourceObj, tableNames, entityTypes)
    //     }
    //   }
    // }
  }

  // async function effectTable(mlsResourceObj, tableNames, entityTypes) {
  //   const tableName = makeTableName(mlsResourceObj.name)
  //   const entityType = entityTypes.find(x => x.$.Name === mlsResourceObj.name)
  //   const indexes = getIndexes(mlsResourceObj.name)
  //   if (tableNames.includes(tableName)) {
  //     await syncTableFields(mlsResourceObj, entityType, indexes)
  //   } else {
  //     await createTable(mlsResourceObj, entityType, indexes)
  //     await createIndexes(tableName, indexes, mlsResourceObj.name)
  //   }
  // }

  // async function createTable(mlsResourceObj, entityType, indexes) {
  //   const fieldsString = entityType.Property
  //     .filter(property => shouldIncludeField(property.$.Name, indexes, platformAdapter.shouldIncludeMetadataField, mlsResourceObj.select))
  //     .map(x => buildColumnString(mlsResourceObj.name, x)).join(", \n")
  //   const tableName = makeTableName(mlsResourceObj.name)
  //   const sql = `CREATE TABLE \`${tableName}\` (
  //     ${fieldsString}
  //   )`
  //   await db.raw(sql)
  // }

  // async function createIndexes(tableName, indexesToAdd, mlsResourceName) {
  //   for (const [indexName, indexProps] of Object.entries(indexesToAdd)) {
  //     const fieldNamesString = indexProps.fields.map(x => `\`${makeFieldName(mlsResourceName, x)}\``).join(', ')
  //     const indexType = indexProps.isPrimary ? 'PRIMARY KEY' : 'INDEX'
  //     const sql = `ALTER TABLE \`${tableName}\` ADD ${indexType} ${indexName} (${fieldNamesString})`
  //     await db.raw(sql)
  //   }
  // }

  // async function syncTableFields(mlsResourceObj, entityType, indexes) {
  //   const tableName = makeTableName(mlsResourceObj.name)
  //   const [rows] = await db.raw(`DESCRIBE \`${tableName}\``)
  //   const tableFields = rows.map(x => x.Field)

  //   const tableFieldNamesObj = _.reduce(tableFields, (a, v) => {
  //     a[v] = true
  //     return a
  //   }, {})
  //   const metadataFieldNamesObj = _.reduce(entityType.Property.map(x => makeFieldName(mlsResourceObj.name, x.$.Name)), (a, v) => {
  //     a[v] = true
  //     return a
  //   }, {})

  //   // In my first production case, this is hosing me. I'm transforming Media records into my own field names, and those
  //   // field names are causing this code to throw. I don't currently need warnings myself, so I will punt on creating a
  //   // warning system.
  //   //
  //   // // If there are any fields in our database that aren't in the metadata, let's warn.
  //   // for (const tableFieldName in tableFieldNamesObj) {
  //   //   if (!(tableFieldName in metadataFieldNamesObj)) {
  //   //     if (tableFieldName === 'id') {
  //   //       continue
  //   //     }
  //   //     // Throw for now, until we set up a warning system.
  //   //     throw new Error(`Table field ${tableFieldName} is not in MLS metadata`)
  //   //   }
  //   // }

  //   for (const metadataProperty of entityType.Property) {
  //     const tableFieldName = makeFieldName(mlsResourceObj.name, metadataProperty.$.Name)
  //     if (!(tableFieldName in tableFieldNamesObj)) {
  //       if (shouldIncludeField(metadataProperty.$.Name, indexes, platformAdapter.shouldIncludeMetadataField, mlsResourceObj.select)) {
  //         const typeString = getDatabaseType(metadataProperty)
  //         const nullableString = metadataProperty.$.Nullable === 'false' ? '' : 'NULL'
  //         await db.raw(`ALTER TABLE \`${tableName}\` ADD COLUMN \`${tableFieldName}\` ${typeString} ${nullableString}`)
  //       }
  //     }
  //   }
  // }

  function getDatabaseType(property) {
    if (platformDataAdapter.overridesDatabaseType(property)) {
      return platformDataAdapter.getDatabaseType(property)
    }

    const type = property.$.Type;
    if (type === 'Edm.Double') {
      const precision = parseInt(property.$.Precision, 10)
      if (precision <= 23) {
        return `FLOAT(${precision})`
      } else {
        return `DOUBLE(${precision})`
      }
    } else if (type === 'Edm.Decimal') {
      return 'DECIMAL(' + property.$.Precision + ', ' + property.$.Scale + ')'
    } else if (type === 'Edm.Boolean') {
      return 'BOOL'
    } else if (type === 'Edm.Date') {
      return 'DATE'
    } else if (type === 'Edm.Int32') {
      return 'INT'
    } else if (type === 'Edm.DateTimeOffset') {
      return 'DATETIME(3)'
    } else if (type === 'Edm.Int64') {
      return 'BIGINT'
    } else if (type === 'Edm.String') {
      if (!property.$.MaxLength) {
        return 'TEXT'
      }
      const maxLength = parseInt(property.$.MaxLength, 10)
      if (maxLength > 255) {
        return 'TEXT'
      }
      return `VARCHAR(${maxLength})`
    } else if (type === 'Edm.GeographyPoint') {
      return 'JSON'
    } else {
      throw new Error('Unknown type: ' + type)
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

  function makeForeignKeyFieldName(parentMlsResourceName, mlsResourceName, name) {
    return userMakeForeignKeyFieldName ? userMakeForeignKeyFieldName(parentMlsResourceName, mlsResourceName, name) : makeName(name)
  }

  function transform(mlsResourceName, record, metadata) {
    return userTransform ? userTransform(mlsResourceName, record, metadata) : record
  }

  function shouldSyncTableSchema(mlsResourceName) {
    return userShouldSyncTableSchema ? userShouldSyncTableSchema(mlsResourceName) : true
  }

  // 'property' is from the RESO Web API XML metadata data dictionary
  function buildColumnString(mlsResourceName, property) {
    const dbType = getDatabaseType(property)
    let sql = `\`${makeFieldName(mlsResourceName, property.$.Name)}\` ${dbType}`
    return sql
  }

  async function syncData(mlsResourceObj, mlsData, metadata, transaction = null) {
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
    //   // Filter out the 'expand' values, which we handle with a recursive call below.
    //   .filter(fieldName => !mlsResourceObj.expand || !mlsResourceObj.expand.map(sub => sub.fieldName).includes(fieldName))
    const transformedMlsData = mlsData.map(x => {
      const val = _.pick(x, fieldNames)
      const partitionKeyVal = val[getPrimaryKeyField(mlsResourceObj.name, indexes)];
      val.id = partitionKeyVal;
      return transform(mlsResourceObj.name, val, metadata);
      // return {
      //   operationType: "Upsert",
      //   partitionKey: partitionKeyVal,
      //   resourceBody: transform(mlsResourceObj.name, val, metadata),
      // }
    })

    // const jobs = [];
    console.log('operation count: ', transformedMlsData.length);

    for (let i = 0; i < transformedMlsData.length; i++) {
      await container.items.upsert(transformedMlsData[i]);
    }

    // while (transformedMlsData.length) {
    //   // delay to avoid getting rate limited
    //   const rows = transformedMlsData.splice(0, 10);
    //   await container.items.bulk(rows);
    //   await delay(1000);
    // }

    // await Promise.all(jobs);
  }

  // function delay(time) {
  //   return new Promise(resolve => setTimeout(resolve, time));
  // }

  async function getTimestamps(mlsResourceName, indexes) {
    const tableName = makeContainerId(mlsResourceName)
    const updateTimestampFields = _.pickBy(indexes, v => v.isUpdateTimestamp)
    const fieldsString = _.map(updateTimestampFields, (v, k) => `MAX(${tableName}.${makeFieldName(mlsResourceName, k)}) as ${k}`).join(', ')
    const partitionKey = getPartitionKey(mlsResourceName)
    const container = await createIfNotExists(tableName, partitionKey);
    // const container = database.container(tableName);
    const querySpec = {
      query: `SELECT ${fieldsString} FROM ${tableName}`
    }
    console.log(`querying ${mlsResourceName} for timestamps: ${querySpec.query}`);
    const { resources: items } = await container.items.query(querySpec).fetchAll();
    // TO DO - Descide how long I want to query records back
    return _.mapValues(updateTimestampFields, (v, k) => items[0][k] || new Date(new Date().setFullYear(new Date().getFullYear() - 1)))
    // return _.mapValues(updateTimestampFields, (v, k) => items[0][k] || new Date(0))
  }

  async function getAllIds(mlsResourceName, indexes) {
    const tableName = makeContainerId(mlsResourceName)
    const officialFieldName = getPrimaryKeyField(mlsResourceName, indexes)
    const userFieldName = makeFieldName(mlsResourceName, officialFieldName)
    const partitionKey = getPartitionKey(mlsResourceName)
    const container = await createIfNotExists(tableName, partitionKey);
    // const container = database.container(tableName);
    const querySpec = {
      query: `SELECT ${tableName}.${userFieldName} AS id FROM ${tableName} ORDER BY ${userFieldName}`
    }
    const { resources: items } = await container.items.query(querySpec).fetchAll();
    const ids = items.map(x => x.id)
    return ids
  }

  async function getCount(mlsResourceName) {
    const tableName = makeContainerId(mlsResourceName)
    const partitionKey = getPartitionKey(mlsResourceName)
    const container = await createIfNotExists(tableName, partitionKey);
    // const container = database.container(tableName);

    const sql = `SELECT COUNT(1) as count FROM ${tableName}`
    const { resources: items } = await container.items.query({ query: sql }).fetchAll();
    return items[0].count
  }

  async function getMostRecentTimestamp(mlsResourceName) {
    const tableName = makeContainerId(mlsResourceName)
    const partitionKey = getPartitionKey(mlsResourceName)
    const container = await createIfNotExists(tableName, partitionKey);
    // const container = database.container(tableName);

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

  async function purge(mlsResourceObj, idsToPurge, getIndexes) {
    // const mlsResourceName = mlsResourceObj.name
    // const tableName = makeTableName(mlsResourceName)
    // const indexes = getIndexes(mlsResourceName)
    // const officialFieldName = getPrimaryKeyField(mlsResourceName, indexes)
    // const userFieldName = makeFieldName(mlsResourceName, officialFieldName)
    // const sql = `DELETE FROM ${tableName} WHERE ${userFieldName} IN (?)`
    // return db.transaction(async trx => {
    //   await trx.raw(sql, [idsToPurge])
    //   if (mlsResourceObj.expand) {
    //     for (const expandedMlsResourceObj of mlsResourceObj.expand) {
    //       if (expandedMlsResourceObj.purgeFromParent) {
    //         await purgeFromParent(mlsResourceObj.name, expandedMlsResourceObj.name, idsToPurge, officialFieldName, trx)
    //       }
    //     }
    //   }
    // })
  }

  // I'm not loving this way of doing things. But to explain it:
  // My current use case is to purge Media records that were originally synced as part of syncing Property records with
  // the expand feature. We need to delete from the Media table using the ResourceRecordKey field (or, what the user
  // maps it to in their table), using the parentIds from the parent table (Property).
  async function purgeFromParent(parentMlsResourceName, mlsResourceName, parentIds, officialFieldName, transaction) {
    // const tableName = makeTableName(mlsResourceName)
    // const userFieldName = makeForeignKeyFieldName(parentMlsResourceName, mlsResourceName, officialFieldName)
    // // TODO: Loop this, say, for each 1,000 records.
    // const sql = `DELETE FROM ${tableName} WHERE ${tickQuote(userFieldName)} IN (?)`
    // return transaction.raw(sql, [parentIds])
  }

  async function closeConnection() {
    // return db.destroy()
  }

  function tickQuote(term, tableName) {
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
      .map((term) => tickQuote(term, tableName))
      .join(', ')
    const sql = `SELECT ${fieldNamesSql} FROM ${tableName} ORDER BY ${tableName}.${userFieldName}`
    const partitionKey = getPrimaryKeyField(mlsResourceName, indexes)
    const container = await createIfNotExists(tableName, partitionKey);
    // const container = database.container(tableName);
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

const axios = require('axios')
const concatStream = require('concat-stream')
const ProgressBar = require('progress')
const moment = require('moment')
const fsPromises = require('fs').promises
const pathLib = require('path')
const _ = require('lodash')

const fileNameTimestampFormatString = 'YYYY-MM-DD-T-HH-mm-ss-SSS'

function catcher(msg, { destinationManager, logger } = {}) {
  return function(error) {
    logger.error({ err: error, location: msg })

    let p = Promise.resolve()
    if (destinationManager) {
      p = destinationManager.closeConnections()
    }
    p.then(() => {
      process.exit(1)
    })
  }
}

// Inspired by https://futurestud.io/tutorials/axios-download-progress-in-node-js
async function fetchWithProgress(axiosConfig, progressCb) {
  const { data, headers } = await axios({
    ...axiosConfig,
    responseType: 'stream',
  })

  return new Promise((resolve, reject) => {
    const contentLength = headers['content-length']
    if (contentLength) {
      const progressBar = new ProgressBar('-> downloading [:bar] :percent :etas', {
        width: 40,
        complete: '=',
        incomplete: ' ',
        total: parseInt(contentLength),
      })

      data.on('data', chunk => {
        progressBar.tick(chunk.length)
      })
    }

    function done(val) {
      // console.log('contentLength', contentLength)
      // console.log('count', count)
      // console.log('length', length)
      // console.log('val.length', val.length)
      resolve(val)
    }
    const concatter = concatStream({ encoding: 'string' }, done)
    data.pipe(concatter)
    data.on('error', reject)
  })
}

function unpackErrorForSerialization(e) {
  // return JSON.stringify(e, Object.getOwnPropertyNames(e))
  return {
    message: e.message,
    stack: e.stack,
  }
}

// This returns an array of arrays. The outer array has an element per MLS resource. Each element is an array of the
// files for that MLS resource.
async function getSourceFiles(mlsSourceName, mlsResources) {
  const filesPerMlsResource = await Promise.all(mlsResources.map(mlsResourceObj => getMlsResourceDirFiles(mlsSourceName, mlsResourceObj.name)))
  return filesPerMlsResource
}

// Batch type should be 'sync' or 'purge'.
function getSourceFilesForBatch(sourceFiles, batchId, batchType) {
  return sourceFiles.map(filesForMlsResource => {
    if (filesForMlsResource === null) {
      return []
    }
    return filesForMlsResource.filter(file => {
      return pathLib.basename(file).startsWith(batchType + '_batch_' + batchId)
    })
  })
}

async function mkdirIfNotExists(dirPath) {
  try {
    await fsPromises.access(dirPath)
  } catch (e) {
    if (e.code === 'ENOENT') {
      await fsPromises.mkdir(dirPath, {
        recursive: true,
        mode: 0o775,
      })
    } else {
      throw e
    }
  }
}

async function deleteFilesForMlsResource(sourceFilesForMlsResource, logger) {
  const shouldMoveFiles = 'debug' === 'debug'
  if (shouldMoveFiles) {
    if (sourceFilesForMlsResource.length) {
      const doneDir = pathLib.join(pathLib.dirname(sourceFilesForMlsResource[0]), 'done')
      await mkdirIfNotExists(doneDir)
      for (const filePath of sourceFilesForMlsResource) {
        const newPath = pathLib.join(doneDir, pathLib.basename(filePath))
        await fsPromises.rename(filePath, newPath)
      }
    }
  } else {
    for (const filePath of sourceFilesForMlsResource) {
      logger.info({ dataFilePath: filePath }, 'Deleting')
      await fsPromises.unlink(filePath)
    }
  }
}

async function deleteSourceFilesForBatch(mlsSourceName, mlsResources, batchType, batchId, logger) {
  const filesPerMlsResource = await getSourceFiles(mlsSourceName, mlsResources)
  const sourceFilesForBatch = getSourceFilesForBatch(filesPerMlsResource, batchId, batchType)
  for (const sourceFilesForMlsResource of sourceFilesForBatch) {
    await deleteFilesForMlsResource(sourceFilesForMlsResource, logger)
  }
}

function getOldestBatchId(filesPerMlsResource, batchType) {
  let oldestBatchTimestamp = null
  for (const filesArray of filesPerMlsResource) {
    if (filesArray === null) {
      continue
    }
    for (const filePath of filesArray) {
      const fileName = pathLib.basename(filePath)
      const regex = new RegExp(batchType + '_batch_(.*)_seq')
      const match = fileName.match(regex)
      if (!match) {
        continue
      }
      const batchId = match[1]
      const timestampFromId = convertBatchIdToTimestamp(batchId)
      if (oldestBatchTimestamp === null || timestampFromId.isBefore(oldestBatchTimestamp)) {
        oldestBatchTimestamp = timestampFromId
      }
    }
  }
  if (!oldestBatchTimestamp) {
    return null
  }
  return convertTimestampToBatchId(oldestBatchTimestamp)
}

function convertTimestampToBatchId(timestamp) {
  return timestamp.format(fileNameTimestampFormatString) + 'Z'
}

function convertBatchIdToTimestamp(batchId) {
  return moment.utc(batchId, fileNameTimestampFormatString)
}

async function getMlsResourceDirFiles(mlsSourceName, mlsResourceName) {
  const dirPath = getMlsResourceDir(mlsSourceName, mlsResourceName)
  try {
    await fsPromises.access(dirPath)
  } catch (e) {
    return null
  }
  const items = (await fsPromises.readdir(dirPath, { withFileTypes: true }))
    .filter(item => !item.isDirectory())
    .map(item => item.name)
  const sortedItems = naturalSort(items)
  const sortedFilePaths = sortedItems.map(x => pathLib.join(dirPath, x))
  return sortedFilePaths
}

function getMlsResourceDir(mlsSourceName, mlsResourceName) {
  const dirPath = pathLib.resolve(__dirname, `../../config/sources/${mlsSourceName}/downloadedData/${mlsResourceName}`)
  return dirPath
}

const collator = new Intl.Collator('en', { numeric: true, sensitivity: 'base', ignorePunctuation: true })

const naturalSort = (array, selector, options) => {
  let a = array.slice()
  if (selector) {
    a = a.sort((a, b) => collator.compare(selector(a), selector(b)))
  } else {
    a.sort((a, b) => collator.compare(a, b))
  }
  if (!selector) {
    options = selector
  }
  if (options && options.reverse) {
    a.reverse()
  }
  return a
}

function getPrimaryKeyField(mlsResourceName, indexes) {
  const keysObj = _.pickBy(indexes, v => v.isPrimary)
  const keyNames = Object.keys(keysObj)
  if (keyNames.length !== 1) {
    throw new Error(`Expected exactly 1 key, got ${keyNames.length} (${keyNames.join(', ')})`)
  }
  const keyName = keyNames[0]
  const fieldNames = keysObj[keyName].fields
  if (fieldNames.length !== 1) {
    throw new Error(`Expected exactly 1 field, got ${fieldNames.length} (${fieldNames.join(', ')})`)
  }
  const officialFieldName = fieldNames[0]
  return officialFieldName
}

function flattenExpandedMlsResources(mlsResources) {
  const all = mlsResources.reduce((a, v) => {
    // Note how we keep the entire object. I feel like we shouldn't lose the original value,
    // with its sub resources and all. But consumers of this method are expected to only look one
    // level deep (as in, not to recurse into the 'expand' property).
    a = a.concat(v)
    if (v.expand) {
      const x = flattenExpandedMlsResources(v.expand)
      a = a.concat(x)
    }
    return a
  }, [])
  const unique = _.uniqBy(all, x => x.name)
  return unique
}

module.exports = {
  catcher,
  fetchWithProgress,
  unpackErrorForSerialization,
  getMlsResourceDirFiles,
  getMlsResourceDir,
  getOldestBatchId,
  convertTimestampToBatchId,
  convertBatchIdToTimestamp,
  getSourceFiles,
  getSourceFilesForBatch,
  deleteSourceFilesForBatch,
  deleteFilesForMlsResource,
  getPrimaryKeyField,
  flattenExpandedMlsResources,
  mkdirIfNotExists,
}

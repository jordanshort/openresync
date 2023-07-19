const { getMlsSourceUserConfig } = require('../../config')

module.exports = function() {
  function getEntityTypes(schemas) {
    return schemas.find(x => x.$.Namespace === 'Odata.Models').EntityType
  }

  function shouldIncludeJsonField(fieldName) {
    // I'm reusing the shouldIncludeMetadataField method because it currently matches the functionality that I want.
    // But it's not necessarily that way. See the BridgeInteractive platform adapter as an example.
    // So, don't be afraid to split this out if necessary.
    if (fieldName === 'Directions') return false;
    
    return shouldIncludeMetadataField(fieldName)
  }

  function shouldIncludeMetadataField(fieldName) {
    // We could filter out fields that have an Annotation where their StandardName is blank.
    // I'm assuming this means it's specific to Trestle.
    // I'm not sure if people want such data so I'll leave it in for now.

    if (fieldName.startsWith('X_')) {
      return false
    }
    return null
  }

  async function fetchAuth(userConfig, mlsSourceName) {
    const sourceConfig = getMlsSourceUserConfig(userConfig, mlsSourceName)
    return {
      accessToken: sourceConfig.accessToken,
      // As far into the future as possible
      expiresAt: 2147483647 * 1000,
    }
  }

  return {
    getEntityTypes,
    shouldIncludeJsonField,
    shouldIncludeMetadataField,
    fetchAuth,
  }
}

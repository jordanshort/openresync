// You are encouraged to think of this config file from a programmatic standpoint, as opposed to static configuration.
// For example, you might wish to reuse code that builds endpoint URLs. Such an idea is shown in this example config.
// You can see how getResourceEndpoint, getReplicationEndpoint, and getPurgeEndpoint all make use of the specific
// source's getResourceEndpoint function later in the file.


// All config values are required unless noted as optional.

// const pathLib = require('path')

module.exports = () => ({
  // Use the expected config version. This allows us to detect if you are using improper config.
  userConfigVersion: '0.2.0',

  // sources is an array of objects. Each object is a "source", or a connection to an MLS. However, there is nothing
  // preventing you from connecting to an MLS multiple times if necessary. For example, from a single MLS, if you wanted
  // Property resources for the city of Georgetown to go to a MySQL destination table named PropertyA and Property
  // resources for the city of Austin to go to a MySQL destination table named Property B, you'd use two different
  // resources. For convenience, you could share more of their configuration.
  sources: [
    {
      name: 'utahRealEstate',

      accessToken: process.env.UTAH_REAL_ESTATE_ACCESS_TOKEN,

      // What's the URL to download the metadata XML?
      metadataEndpoint: 'https://resoapi.utahrealestate.com/reso/odata/$metadata',

      // metadataPath is mainly for debug uses. If you want to speed up your syncs, perhaps during your initial testing
      // phase, you could save the metadata locally on your system and use that file. However, you'll want to not use
      // this value in production.
      // metadataPath: pathLib.resolve(__dirname, 'sources/abor_bridge_interactive/actris_ref_metadata.xml'),

      // getResourceEndpoint is used in non replication scenarios, such as to display stats on the website like how
      // many records are in the source MLS system. You may include $filter values if desired, such as:
      // $filter=PropertyType eq 'Residential'
      // The function accepts an object, which is of type MlsResource, described in the 'mlsResources' item below.
      getResourceEndpoint: utahRealEstate.getResourceEndpoint,

      // Get the replication endpoint for a given MLS Resource. This should be a function that returns a string.
      // The function accepts an object, which is of type MlsResource, described in the 'mlsResources' item below.
      // You may include a $filter query parameter, but that $filter query parameter will be appended (using an AND
      // condition) with timestamps by the openresync application.
      getReplicationEndpoint: mlsResourceObj => {
        const resourceEndpoint = utahRealEstate.getResourceEndpoint(mlsResourceObj)
        const url = new URL(resourceEndpoint)
        return url.toString()
      },

      // Get the replication endpoint for a given MLS Resource, but meant for purging. This should be a function that
      // returns a string.
      // The function accepts two parameters: 1) MlsResource object, as described in the 'mlsResources' item below.
      // 2) isExpandedMlsResource, a boolean which indicates if the resource is a root or was used as an $expand'd
      // resource. In the latter case, you might perhaps not want to use any $filter condition.
      // Do not include a $select query string parameter; it will be overwritten with the primary key(s) of the
      // resource.

      getPurgeEndpoint: (mlsResourceObj) => {
        return utahRealEstate.getPurgeEndpoint(mlsResourceObj)
      },

      // This should be the largest $top value allowed by the MLS during replication. Utah Real Estate allows 200.
      top: 200,

      // This should be the largest $top value allowed by the MLS during replication. Utah Real Estate allows 200.
      topForPurge: 200,

      // Utah real estate allows $orderby during replication.
      useOrderBy: false,

      reconcileFilterTemplate: 'PLACEHOLDER',

      reconcileFilterSeparator: ' or ',

      getReconcileIdFilterString: (primaryKey, separator, id) => {
        return `${separator}${primaryKey} eq '${id}'`
      },

      // Specify the name of the platform. This will determine things like
      // how authenticatoin is handled, how the XML metadata is interpreted, which fields are ignored, etc.
      platformAdapterName: 'utahRealEstate',

      // mlsResources is an array of objects. Each object represents a resource in the MLS, such as Property, Member,
      // etc. The name is the case sensitive name from the MLS.
      // Other optional fields are
      //   * select: used for $select on the replication endpoint. An array of strings that are the field names.
      //   * expand: used for $expand. An array of objects, where each object is a subresource. Has these properties:
      //     * name: The TYPE of the subresource, for example Property might have an $expand of BuyerAgent. In this
      //             case, the TYPE of the subresource maps to Member, so set the name to Member.
      //     * fieldName: with the example above where Property has an $expand of BuyerAgent, the 'fieldName' would be
      //                  BuyerAgent
      //     The name and fieldName might be identical, for example Property has an $expand of Media whose fieldName is
      //     also Media.
      mlsResources: [
        {
          name: 'Property',
          expand: [
            {
              fieldName: 'Media',
              name: 'Media'
            },
          ],
        },
      ],

      destinations: [
        {
          // Here's an example of a destination using the mysql data adapter.

          // The type of the destination, which is 'mysql' or 'solr'.
          type: 'mysql',

          // The name should be thought of as an ID, meant for computers. It is arbitrary but must be unique among
          // destinations.
          name: 'mysql1',

          // This is the config block specifically meant for the destination. It differs per destination. Here's an
          // example for MySQL.
          config: {
            // The username, password, host, port, database name all wrapped into one.
            connectionString: 'mysql://realconnect:Jmoney15!rco123@idx.mysql.database.azure.com:3306/idx',

            // Optional
            // makeTableName allows you to change the name of the table used. It takes the name of the resource and
            // you should return a string. An example use case is if you are using a single database for multiple MLS
            // sources, you might not want each to use the name 'Property' for a table name, so you could use a prefix
            // perhaps based on the name of the MLS, e.g. abor_Property (for Austin Board of Realtors) and
            // crmls_Property (for California Regional MLS). This is optional and if not specified, the resource name
            // will be used.
            makeTableName: name => 'ure_' + name,

            // Optional
            // makeFieldName allows you to change the name of the field used. RESO Web API field names are
            // PascalCase, but you might prefer, for example, snake_case.
            // Note: if you use the transform function, described below, the field names your function will receive are
            // the keys from the object you return from transform().
            // makeFieldName: name => 'myfieldprefix_' + name,

            // Optional
            // shouldSyncTableSchema allows you to opt out of a table's schema being synced. The MySQL data adapter
            // synchronizes table schema, which is handy when new fields are added or removed
            // over time. It takes the MLS resource name and should return a boolean. Default returns true.
            // If you return false, it means that you are responsible for creating the table as well as altering.
            // shouldSyncTableSchema: function(mlsResourceName) {
            //   if (mlsResourceName === 'Media') {
            //     return false
            //   }
            //   return true
            // }

            // Optional
            // makeForeignKeyFieldName is used in the purge process. If, in the mlsResources section above, you use the
            // 'expand' property to sync subresources, and the primary key of the subresource's table differs from the
            // MLS's, you will need to use this. It's a function that takes in the parent MLS resource name, the sub-
            // resource name, and the field name, and returns the name of your primary key.
            // makeForeignKeyFieldName: (parentMlsResourceName, mlsResourceName, fieldName) => {
            //   if (mlsResourceName === 'Media') {
            //     if (parentMlsResourceName === 'Property') {
            //       if (fieldName === 'ListingKey') {
            //         return 'Content-ID'
            //       }
            //     }
            //   }
            //   return fieldName
            // }

            // Optional
            // transform allows you to change the record of what would be inserted/updated in the database. If the only
            // difference between what you want inserted/updated is the field names, then you should use the
            // makeFieldName option. But this function would allow you to modify the data in any way, for example change
            // keys, values, add key/value pairs, remove some, etc. It takes the MLS resource name, the record as
            // downloaded, and the metadata object, and should return an object.
            // Note: For the primary key's value, you may return null if your table's primary key is auto-incremented,
            // which is the default.
            // transform: (mlsResourceName, record, metadata) => {
            //   // Return an object. Do not mutate record.
            // }
          },
        },
        // {
        //   type: 'cosmos',
        //   name: 'cosmos1',
        //   config: {
        //     endpointUri: process.env.WFRMLS_ENDPOINT,
        //     key: process.env.AZURE_ACCESS_KEY,
        //     databaseId: 'IDX',
        //     // this sets the container name in cosmos
        //     makeTableName: name => 'URE_' + name,
        //     // Optional
        //     // makeFieldName allows you to change the name of the field used. RESO Web API field names are
        //     // PascalCase, but you might prefer, for example, snake_case.
        //     // Note: if you use the transform function, described below, the field names your function will receive are
        //     // the keys from the object you return from transform().
        //     // makeFieldName: name => 'myfieldprefix_' + name,

        //     // Optional
        //     // shouldSyncTableSchema allows you to opt out of a table's schema being synced. The MySQL data adapter
        //     // synchronizes table schema, which is handy when new fields are added or removed
        //     // over time. It takes the MLS resource name and should return a boolean. Default returns true.
        //     // If you return false, it means that you are responsible for creating the table as well as altering.
        //     // don't need to for cosmos
        //     shouldSyncTableSchema: function(mlsResourceName) {
        //       return false;
        //     }

        //     // Optional
        //     // makeForeignKeyFieldName is used in the purge process. If, in the mlsResources section above, you use the
        //     // 'expand' property to sync subresources, and the primary key of the subresource's table differs from the
        //     // MLS's, you will need to use this. It's a function that takes in the parent MLS resource name, the sub-
        //     // resource name, and the field name, and returns the name of your primary key.
        //     // makeForeignKeyFieldName: (parentMlsResourceName, mlsResourceName, fieldName) => {
        //     //   if (mlsResourceName === 'Media') {
        //     //     if (parentMlsResourceName === 'Property') {
        //     //       if (fieldName === 'ListingKey') {
        //     //         return 'Content-ID'
        //     //       }
        //     //     }
        //     //   }
        //     //   return fieldName
        //     // }

        //     // Optional
        //     // transform allows you to change the record of what would be inserted/updated in the database. If the only
        //     // difference between what you want inserted/updated is the field names, then you should use the
        //     // makeFieldName option. But this function would allow you to modify the data in any way, for example change
        //     // keys, values, add key/value pairs, remove some, etc. It takes the MLS resource name, the record as
        //     // downloaded, and the metadata object, and should return an object.
        //     // Note: For the primary key's value, you may return null if your table's primary key is auto-incremented,
        //     // which is the default.
        //     // transform: (mlsResourceName, record, metadata) => {
        //     //   // Return an object. Do not mutate record.
        //     // }
        //   },
        // },
      ],

      // Optional
      // Here you can configure when this source and its resources will sync, be purged, or be reconciled (see the
      // README for the difference). It is an object with three properties, sync, purge, and reconcile, which are each
      // objects. Their only property is called cronStrings, and it is an array of cron strings, in the
      // [normal cron format](https://www.npmjs.com/package/cron#cron-ranges). It is technically possible to specify
      // a cron schedule to run too often (like once per second), which should be avoided. A suggested cron schedule for
      // syncing would be `*/15 * * * *`, or every 15 minutes. Check in with your platform to know what's recommended.
      cron: {
        // Optional. If not included, syncs will not be performed.
        sync: {
          // Optional
          // Specify whether sync cron jobs will be run.
          enabled: true,

          // Optional. If not included, syncs will not be performed.
          // Specify an array of cron strings for when the sync cron job(s) should be run.
          cronStrings: ['10,25,40,55 * * * *'],
        },

        // Optional. If not included, purges will not be performed.
        purge: {
          // Optional
          // Specify whether purge cron jobs will be run.
          enabled: true,

          // Optional. If not included, purges will not be performed.
          // Specify an array of cron strings for when the purge cron job(s) should be run.
          cronStrings: ['0,15,30,45 * * * *'],
        },

        // Optional. If not included, reconciles will not be performed.
        reconcile: {
          // Optional
          // Specify whether reconcile cron jobs will be run.
          enabled: true,

          // Optional. If not included, reconciles will not be performed.
          // Specify an array of cron strings for when the reconcile cron job(s) should be run.
          cronStrings: ['5,20,35,50 * * * *']
        },
      },
    },
  ],

  // Optional
  server: {
    // Optional
    // The server runs on this port. Defaults to 4000.
    // port: 4000,
  },

  // This database is used for stats, such as keeping the history of the sync, e.g. when the sync (or purge) occurred,
  // how many records were synced per resource and destination, etc.
  database: {
    connectionString: 'mysql://realconnect:Jmoney15!rco123@idx.mysql.database.azure.com:3306/idx',
    // connectionString: process.env.STAT_DB_CONNECTION_STRING,
  },
})

const utahRealEstate = {
  getResourceEndpoint: mlsResourceObj => {
    const endpoint = `https://resoapi.utahrealestate.com/reso/odata/${mlsResourceObj.name}`;
    const url = new URL(endpoint);
    if (mlsResourceObj.name === 'Property') {
      url.searchParams.set(
        '$filter', 
        "(StandardStatus eq Odata.Models.StandardStatus'Active' or StandardStatus eq Odata.Models.StandardStatus'Pending' or StandardStatus eq Odata.Models.StandardStatus'ActiveUnderContract') and (PropertyType eq Odata.Models.PropertyType'Residential' or PropertyType eq Odata.Models.PropertyType'Land')"
      );
    }
    return url.toString();
  },
  getPurgeEndpoint: mlsResourceObj => {
    const endpoint = `https://resoapi.utahrealestate.com/reso/odata/${mlsResourceObj.name}`;
    const url = new URL(endpoint);
    url.searchParams.set(
      '$count',
      'true'
    );

    if (mlsResourceObj.name === 'Property') {
      url.searchParams.set(
        '$filter', 
        "(StandardStatus eq Odata.Models.StandardStatus'Active' or StandardStatus eq Odata.Models.StandardStatus'Pending' or StandardStatus eq Odata.Models.StandardStatus'ActiveUnderContract') and (PropertyType eq Odata.Models.PropertyType'Residential' or PropertyType eq Odata.Models.PropertyType'Land')"
      );
    }
    return url.toString();
  }
}

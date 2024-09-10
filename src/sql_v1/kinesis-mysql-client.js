"use strict";
//----------------------------------------------------------------------------------------------------
// Packages
//----------------------------------------------------------------------------------------------------
const SqlClient = require('./sql-client');
// const redisData = require('../redis/redis_data');
// const logger = require('../../common/logger');

const KinesisSqlClient = (function () {

  const connectionConfigs = [];
  if (process.env.OPSTOOL__MYSQL_CONNECTION_INFO != undefined) {
    const connectionInfos = JSON.parse(process.env.OPSTOOL__MYSQL_CONNECTION_INFO);
    for (let c of connectionInfos) {
      let connectionLimit = parseInt(process.env.OPSTOOL__MYSQL_CONNECTION_POOLSIZE);
      let weight = (c.weight != undefined) ? c.weight : 10;
      if (c.replicaCount != undefined && c.replicaCount > 1) {
        connectionLimit *= c.replicaCount;
      }

      connectionConfigs.push({
        poolName: c.poolName,
        weight: weight,
        host: c.host,
        port: c.port,
        user: process.env.OPSTOOL__MYSQL_USER,
        password: process.env.OPSTOOL__MYSQL_PASSWORD,
        database: process.env.OPSTOOL__MYSQL_DATABASE,
        connectionLimit: connectionLimit
      });
    }
  }

  const sqlClient = new SqlClient(connectionConfigs);

  async function test_query(query) {
    const result = await sqlClient.execute('write', 'SELECT 1');
    console.log('@test', result);
    return result;
  }

  function initialize() {
    try {
      sqlClient.initialize();
      return true;
    } catch (err) {
      console.error(err.message);
      return false;
    }
  }

  async function reload() {
    // const result = await redisData.getTradeSqlWeight();
    // if (Object.values(result).length == 0) {
    //   return;
    // }

    // sqlClient.updateSqlWeight(result);
  }

  async function insert_kinesiss_log(tableName, date, type, value) {
    try {
      const query = `INSERT INTO ${tableName} (date, type, value) VALUES (?, ?, ?)`;
      const params = [date, type, value];
      const result = await sqlClient.execute('write', query, params);
      return result
    } catch (error) {
      console.error(error.message);
      throw { errorLevel: 'error', errorCode: 'ERR_SQL_INTERNAL' }
    }
  }

  return {
    test_query,
    initialize,
    reload,
    tick: async function () {
      try {
        await reload();
      } catch (error) {
        console.error(`[trade-sql-client] tick ${error.message}`);
      }
    },
    getTimers: function () {
      return [{ type: 'e60s', timer: KinesisSqlClient.tick }];
    },

    insert_kinesiss_log,
  };

})();

//----------------------------------------------------------------------------------------------------
// Exports
//---------------------------------------------------------------------------------------------------- 
module.exports = KinesisSqlClient;

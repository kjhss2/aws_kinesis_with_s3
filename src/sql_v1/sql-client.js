'use strict';

// const sqlQueryMetric = require('./sql-query-metric');
// const logger = require('../logger');
const mysql = require('mysql2/promise');
const { getRandomI } = require('./commonFunction');

const SqlClient = function (connectionConfigs) {
  this.connectionConfigs = connectionConfigs;
}

SqlClient.prototype.initialize = function () {
  try {
    // promise 버전의 PromisePoolCluster를 사용 하지 않은 이유
    // 1. PoolCluster의 일부 인터페이스 미 지원
    // 2. PoolCluster selector의 weight 기반 기능 미 지원으로 특정 pool의 db 부하 분산을 컨트롤 하기 힘듬

    // SqlClient.prototype.execute의 poolName 지정 방법
    // 1. * (모든 pool에서 가중치 기반으로 pool 선택)
    // 2. read-1 (poolName에 맞는 pool 선택)
    // 3. read-* (poolName이 "read-"가 포함 되는 pool 중에 가중치 기반으로 pool 선택)

    this.pools = {};
    this.poolWeights = [];
    // this.pools.totalWeight = 0;

    if (this.connectionConfigs.length == 0) {
      throw new Error('[sql] connection info empty');
    }

    for (let config of this.connectionConfigs) {
      if (this.pools[config.poolName] != undefined) {
        throw new Error(`duplicate poolName: ${config.poolName}`);
      }

      this.pools[config.poolName] = mysql.createPool({
        host: config.host,
        port: config.port,
        user: config.user,
        password: config.password,
        database: config.database,
        connectionLimit: 0,
        waitForConnections: true,
        multipleStatements: true,
        timezone: '+00:00'
      });

      this.pools[config.poolName].host = config.host;
      this.pools[config.poolName].port = config.port;
      this.pools[config.poolName].user = config.user;
      this.pools[config.poolName].password = config.password;
      this.pools[config.poolName].database = config.database;
      this.pools[config.poolName].poolName = config.poolName;

      this.pools.totalWeight += config.weight;
      this.poolWeights.push({
        database: config.database,
        poolName: config.poolName,
        weight: config.weight
      });

      if (config.poolName == "read") {
        // AutoScaling Policy에 의해서 동적으로 생성된 인스턴스를 이용 하려면
        // 쿼리를 refreshCount 만큼 실행 한 후에 connection를 종료 한다.
        this.pools[config.poolName].on('acquire', function (connection) {
          if (connection.bh_acquire_count == undefined) {
            connection.bh_acquire_count = 1;
          }
          else {
            ++connection.bh_acquire_count;
          }
        });

        this.pools[config.poolName].on('release', function (connection) {
          let refreshCount = 1500;
          if (process.env.MAX_SQL_CONNECTION_ACQUIRE_COUNT) {
            refreshCount = parseInt(process.env.MAX_SQL_CONNECTION_ACQUIRE_COUNT);
          }

          if (connection.bh_acquire_count < refreshCount) {
            return;
          }

          connection.destroy();
        });
      }

      // 일단 반만 확보 한다.
      const count = parseInt(config.connectionLimit);
      for (let i = 0; i < count; ++i) {
        this.pools[config.poolName].query('SELECT 1');
      }
    }

  } catch (error) {
    throw error;
  }
}

SqlClient.prototype.getDefaultPoolName = function () {
  if (this.poolWeights.length == 0) {
    throw new Error('empty sql pool');
  }

  return this.poolWeights[0].poolName;
}

SqlClient.prototype.test = function () {

  const total = 10000;
  const callInfos = {};
  for (let i = 0; i < total; ++i) {
    const picked = this.randomPick();
    if (callInfos[picked] == undefined) {
      callInfos[picked] = 1;
    }

    callInfos[picked] += 1;
  }

  for (let c in callInfos) {
    const count = callInfos[c];
    console.log(`pool ${c} : ${(count / total * 100).toFixed(2)}`);
  }
}

SqlClient.prototype.updateSqlWeight = function (weights) {
  for (let key in weights) {
    const poolWeight = this.poolWeights.find(element => element.poolName == key);
    if (poolWeight == undefined) {
      console.warn(`[SqlClient][Weight] not exist poolName ${key} `);
      continue;
    }

    const weight = parseInt(weights[key]);
    if (poolWeight.weight == weight) {
      continue;
    }

    if (weight < 0) {
      console.warn(`[SqlClient][Weight][${poolWeight.database}] ${poolWeight.poolName} invalid value ${weight} `);
      continue;
    }

    const prev = poolWeight.weight;
    poolWeight.weight = weight;

    console.debug(`[SqlClient][Weight][${poolWeight.database}] poolName: ${poolWeight.poolName}, prev: ${prev}, updated: ${poolWeight.weight} `);
  }
}

SqlClient.prototype.randomPick = function () {
  const defaultPoolName = this.getDefaultPoolName();

  const totalWeight = this.poolWeights.reduce((acc, element) => acc + element.weight, 0);
  const randomValue = getRandomI(totalWeight);

  let value = 0;
  for (let p of this.poolWeights) {
    value += p.weight;
    if (value > randomValue) {
      return p.poolName;
    }
  }

  return defaultPoolName;
}

SqlClient.prototype.widecardPick = function (keyword) {
  const filtered = this.poolWeights.filter(element => {
    if (element.weight == 0) {
      return false;
    }

    return element.poolName.startsWith(keyword)
  });

  if (filtered.length == 0) {
    return undefined;
  }

  if (filtered.length == 1) {
    return filtered[0].poolName;
  }

  const totalWeight = filtered.reduce((acc, element) => acc + element.weight, 0);
  const randomValue = getRandomI(totalWeight);

  let value = 0;
  for (let p of filtered) {
    value += p.weight;
    if (value > randomValue) {
      return p.poolName;
    }
  }

  console.error(`[widecardPick] keyword: ${keyword}, total: ${totalWeight}, random: ${randomValue}`);
  return undefined;
}

SqlClient.prototype.getPool = function (poolName) {
  let defaultPoolName = this.getDefaultPoolName();

  // cluster가 아니면 poolName 무시한다.
  if (this.poolWeights.length == 1) {
    return this.pools[defaultPoolName];
  }

  if (poolName == '*') {
    const picked = this.randomPick();
    return this.pools[picked];
  }

  if (this.pools[poolName] != undefined) {
    return this.pools[poolName];
  }

  const keyword = poolName.substring(poolName.length - 1, 0);
  const picked = this.widecardPick(keyword);
  if (picked == undefined) {
    throw new Error(`[sql-client] invalid pool name: ${poolName}`);
  }

  return this.pools[picked];
}

SqlClient.prototype.execute = async function (poolName, query, params, extra) {
  try {
    const start = process.hrtime();

    const pool = this.getPool(poolName);
    if (pool == undefined) {
      throw new Error(`[sql-client] invalid pool: ${poolName}`);
    }

    const result = await pool.query(query, params);
    const diff = process.hrtime(start);
    const elapsed = (diff[0] * 1e3 + diff[1] * 1e-6);

    extra = extra || '';

    let slowElapsed = 300;
    if (process.env.SQL_SLOW_QUERY_MS) {
      slowElapsed = parseInt(process.env.SQL_SLOW_QUERY_MS);
    }

    if (elapsed >= slowElapsed) {
      console.warn(`[SqlQueryMetric][slowlog] ${query}, ${JSON.stringify(params)}, elapsed: ${elapsed.toFixed(2)} ms, extra: ${extra}`);
    }

    // const metric = `${pool.database}_${query}`;
    // sqlQueryMetric.add(metric, params, elapsed);

    return result;

  } catch (error) {
    throw error;
  }
}

//----------------------------------------------------------------------------------------------------
// Exports
//---------------------------------------------------------------------------------------------------- 
module.exports = SqlClient;

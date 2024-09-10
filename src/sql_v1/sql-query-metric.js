'use strict'

const fetch = require('node-fetch');
const { Elapsed2RoundedElapsed } = require('./commonFunction');

const SqlQueryMetric = (function () {
  let queries = {};
  let slowQueries = [];

  let isEnable = false;
  let metricManagerUrl = undefined;
  if (process.env.METRIC_MANAGER_URL != undefined) {
    isEnable = true;
    metricManagerUrl = `http://${process.env.METRIC_MANAGER_URL}:3005/metric/api`;
  }
  else {
    console.info('[METRIC: SqlQuery] disabled');
  }

  function reset() {
    queries = {};
    slowQueries = [];
  }

  async function sendMetric() {
    /*
    try {
      if (Object.keys(queries).length == 0) {
        return;
      }

      let url = `${metricManagerUrl}/internal/sql-query`;
      let res = await fetch(url, {
        method: 'post',
        body: JSON.stringify(queries),
        headers: { 'Content-Type': 'application/json' }
      });

      if (res == undefined) {
        return false;
      }

      if (!res.ok) {
        return false;
      }

      const body = await res.json();
      if (body.c != 0) {
        console.error(`[METRIC: SqlQuery] res: ${JSON.stringify(body)}`);
        throw new Error(body.e);
      }

      console.debug(`[METRIC: SqlQuery] data sended. ${JSON.stringify(queries)}`);
      return true;
    } catch (error) {
      console.error(`[METRIC: SqlQuery][sendMetric] error: ${error.message} data: ${JSON.stringify(queries)}`);
      throw error;
    }
    */
  }

  return {
    tick: async function () {
      try {
        if (!isEnable) {
          return false;
        }

        let result = await sendMetric();
        if (!result) {
          return;
        }

        reset();
      } catch (error) {
        console.error(`[METRIC: SqlQuery] error: ${error.message}`);
      }
    },
    add: function (query, params, elapsed) {
      elapsed = parseFloat(elapsed.toFixed(2));

      if (!isEnable) {
        return false;
      }

      // percentile 관련 데이터는 반올림된 값을 사용
      // min, max, avg 관련은 실제 값을 사용 
      let roundedElapsed = Elapsed2RoundedElapsed(elapsed);
      const q = queries[query];
      if (q == undefined) {
        queries[query] = {
          query: query,
          callCount: 1,
          sumElapsed: elapsed,
          minElapsed: elapsed,
          maxElapsed: elapsed,
          elapsedDict: {}
        };

        queries[query].elapsedDict[roundedElapsed] = { roundedElapsed, count: 1 };
        return;
      }

      q.callCount += 1;
      q.sumElapsed += elapsed;

      const elapsedInfo = q.elapsedDict[roundedElapsed];
      if (elapsedInfo == undefined) {
        q.elapsedDict[roundedElapsed] = { roundedElapsed, count: 1 };
      }
      else {
        elapsedInfo.count += 1;
      }

      q.minElapsed = Math.min(q.minElapsed, elapsed);
      q.maxElapsed = Math.max(q.maxElapsed, elapsed);
    },
    getTimers: function () {
      return [{ type: 'd_e60s', timer: SqlQueryMetric.tick }];
    }
  };
})();

module.exports = SqlQueryMetric;

const mysql = require('mysql2/promise');
const { LoginModel } = require('./model/login-model');
const { ItemModel } = require('./model/item-model');
const { ShopModel } = require('./model/shop-model');
const { TradeModel } = require('./model/trade-model');

let database = null;
class MySQL2DBC {
  constructor() {
    if (database) {
      return database;
    }

    const config = {
      host: process.env.STLOG_MYSQL_HOST,
      port: process.env.STLOG_MYSQL_PORT,
      database: process.env.STLOG_MYSQL_DATABASE,
      user: process.env.STLOG_MYSQL_USER,
      password: process.env.STLOG_MYSQL_PASSWORD,
      connectionLimit: process.env.STLOG_MYSQL_CONNECTION_POOLSIZE,
      waitForConnections: true,
    };

    this.pool = mysql.createPool(config);
    database = this;
  }

  async insertDataBatch(records, tableName) {
    const dataModel = this.getDataModel(tableName);
    if (dataModel) {
      // Add a new Data
      return await dataModel.insertDataBatch(records);
    }
  }

  async getDataById(id, table_name = "") {
    const dataModel = this.getDataModel(table_name);
    // get a find data by id
    const data = await dataModel.findDataById(id);
  }

  async getS3FileCursor(tableName, prefixDate) {
    try {
      const sql = `SELECT * FROM s3_file_cursor WHERE table_name = ? AND prefix_date = ?`;
      const [rows] = await this.pool.query(sql, [tableName, prefixDate]);
      return rows[0]?.file_key === undefined ? { table_name: tableName, prefix_date: prefixDate, prefix_hour: '00' } : rows[0];
    } catch (error) {
      console.error(`${error}`);
    }
  }

  async makeInsertS3Objects(fileName, s3Objects = []) {
    if (s3Objects.length === 0) {
      return [];
    }

    try {
      if (fileName === undefined) {
        return s3Objects;
      } else {
        const findIndex = s3Objects.findIndex(s3Object => fileName === s3Object.Key);
        if (findIndex > -1) {
          return s3Objects.slice(findIndex + 1);
        } else {
          return s3Objects;
        }
      }
    } catch (error) {
      console.error(`${error}`);
    }
  }

  async upsertS3FileCursor(tableName, s3Object) {
    const sql = `UPDATE s3_file_cursor SET prefix_date = ?, prefix_hour = ?, file_key = ?, file_size = ?, last_modified = ? WHERE table_name = ?`;
    const params = [
      s3Object.prefixDate,
      s3Object.prefixHour,
      s3Object.Key,
      s3Object.Size,
      s3Object.LastModified,
      tableName,
    ]
    const result = await this.pool.query(sql, params);
    return result;
  }

  // Method to close the pool when done
  async close() {
    try {
      await this.pool.end();
      database = null;
    } catch (error) {
      console.error(`Error closing the database connection:${error}`);
    }
  }

  getDataModel = (tableName = '') => {
    let dataModel = null;
    const tName = tableName.toLowerCase();
    switch (tName) {
      case "stlog_login_action":
        dataModel = new LoginModel(this.pool, tName);
        break;
      case "stlog_shop_action":
        dataModel = new ShopModel(this.pool, tName);
        break;
      case "stlog_trade_action":
        dataModel = new TradeModel(this.pool, tName);
        break;
      case "stlog_item_action":
        dataModel = new ItemModel(this.pool, tName);
        break;

      default:
        break
    }
    return dataModel;
  }
}

module.exports = { MySQL2DBC };
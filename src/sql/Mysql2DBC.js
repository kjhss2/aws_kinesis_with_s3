const mysql = require('mysql2/promise');
const { LoginModel } = require('./model/LoginModel');
const { TradeModel } = require('./model/TradeModel');
const { ShopModel } = require('./model/ShopModel');

let database = null;
class MySQL2DBC {
  constructor() {
    if (database) {
      return database;
    }

    const config = {
      host: process.env.OPSTOOL__MYSQL_HOST,
      user: process.env.OPSTOOL__MYSQL_USER,
      port: process.env.OPSTOOL__MYSQL_PORT,
      password: process.env.OPSTOOL__MYSQL_PASSWORD,
      database: process.env.OPSTOOL__MYSQL_DATABASE,
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
    console.log('@getData', data);
  }

  async checkSaveS3File(s3Objects) {
    // 중복 검사: 이미 처리된 파일은 건너뛰기
    const unprocessedObjects = [];

    if (s3Objects.length > 0) {
      for (const obj of s3Objects) {
        const sql = `SELECT COUNT(*) as count FROM s3_files WHERE file_key = ?`;
        const [rows] = await this.pool.query(sql, [obj.Key]);
        if (rows[0].count === 0) {
          unprocessedObjects.push(obj);
        }
      }
    }

    return unprocessedObjects;
  }

  async insertSaveS3File(s3Object) {
    const sql = `INSERT INTO s3_files (file_key, etag, file_size, last_modified) VALUES (?, ?, ?, ?)`;
    const params = [
      s3Object.Key,
      s3Object.Etag,
      s3Object.Size,
      s3Object.LastModified
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
      console.error('Error closing the database connection:', error);
    }
  }

  getDataModel = (tableName = '') => {
    let dataModel = null;
    const tName = tableName.toLowerCase();
    switch (tName) {
      case "de_login_action":
        dataModel = new LoginModel(this.pool, tName);
        break;
      case "de_shop_action":
        dataModel = new ShopModel(this.pool, tName);
        break;
      case "de_trade_action":
        dataModel = new TradeModel(this.pool, tName);
        break;

      default:
        break
    }
    return dataModel;
  }
}


module.exports = { MySQL2DBC };
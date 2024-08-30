const mysql = require('mysql2/promise');

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

  async insertData(table_name, data) {
    const dataModel = new DataModel(this.pool, table_name);

    try {
      // Add a new Data
      const result = await dataModel.insertData(data);
      console.log('New Data added : ', result);
    } catch (error) {
      console.error('Error:', error);
    }
  }

  async getDatas(table_name) {
    const dataModel = new DataModel(this.pool, table_name);

    try {
      // Add a new Data
      const datas = await dataModel.getAllDatas();
      console.log('@getDatas', datas);
    } catch (error) {
      console.error('Error:', error);
    }
  }

  async getDataById(id, table_name = "DE_SERVER_ACTION") {
    const dataModel = new DataModel(this.pool, table_name);

    try {
      // get a find data by id
      const data = await dataModel.findDataById(id);
      console.log('@getData', data);
    } catch (error) {
      console.error('Error:', error);
    }
  }

  // Method to execute queries
  async query(sql, params) {
    try {
      const [rows, fields] = await this.pool.execute(sql, params);
      return rows;
    } catch (error) {
      console.error('Error executing query:', error);
      throw error;
    }
  }

  // Method to close the pool when done
  async close() {
    try {
      await this.pool.end();
      database = null;
      console.log('@pool close');
    } catch (error) {
      console.error('Error closing the database connection:', error);
    }
  }
}

// Example usage of the Database class
class DataModel {
  constructor(pool, tableName) {
    this.pool = pool;
    this.tableName = tableName;
  }

  // Retrieve all datas from the database
  async getAllDatas() {
    const sql = `SELECT * FROM ${this.tableName}`;
    return await this.pool.query(sql);
  }

  // Find a data by ID
  async findDataById(id) {
    const sql = `SELECT * FROM ${this.tableName} WHERE id = ?`;
    const users = await this.pool.query(sql, [id]);
    return users[0] || null;
  }

  // Add a new data
  async insertData(data) {
    try {
      const sql = `INSERT INTO ${this.tableName} (contentType, nowDate, base, msg) VALUES (?, ?, ?, ?)`;
      const params = [
        data.contentType,
        data.nowDate,
        JSON.stringify(data.base),
        JSON.stringify(data.msg)
      ]
      const result = await this.pool.query(sql, params);
      return result;
    } catch (error) {
      console.error("InsertData Error : ", error);
    }
  }
}

module.exports = { MySQL2DBC };
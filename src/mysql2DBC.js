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

  async insertData(data) {
    const dataModel = new DataModel(this.pool, data?.msg?.tableName);

    try {
      // Add a new Data
      const result = await dataModel.insertData(data);
      console.log('New Data added : ', result);
    } catch (error) {
      console.error('Error:', error);
    }
  }

  async insertDataBatch(records, tableName) {
    const dataModel = new DataModel(this.pool, tableName);

    try {
      // Add a new Data
      const result = await dataModel.insertDataBatch(records);
      console.log('New Batch Data added : ', result);
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
  // async query(sql, params) {
  //   try {
  //     const [rows, fields] = await this.pool.execute(sql, params);
  //     return rows;
  //   } catch (error) {
  //     console.error('Error executing query:', error);
  //     throw error;
  //   }
  // }

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

  // MySQL에 데이터를 배치로 삽입하는 함수
  async insertDataBatch(records) {
    // const connection = await mysql.createConnection(dbConfig);

    // 데이터를 배치로 삽입하는 쿼리
    const sql = `INSERT INTO ${this.tableName} (contentType, nowDate, base, msg) VALUES ?`;
    const values = records.map(rowData => {
      const record = JSON.parse(rowData);
      return [record.contentType, record.nowDate, JSON.stringify(record.base), JSON.stringify(record.msg)]
    });

    try {
      const [result] = await this.pool.query(sql, [values]);
      console.log(`Inserted ${result.affectedRows} rows into MySQL`);
      return result;
    } catch (error) {
      console.error('Error inserting into MySQL:', error);
    }
  }

}

module.exports = { MySQL2DBC };
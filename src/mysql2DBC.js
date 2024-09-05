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

  async checkSaveS3File(s3Objects) {
    // 중복 검사: 이미 처리된 파일은 건너뛰기
    const unprocessedObjects = [];

    if (s3Objects.length > 0) {
      try {
        for (const obj of s3Objects) {
          const sql = `SELECT COUNT(*) as count FROM s3_files WHERE file_key = ?`;
          const [rows] = await this.pool.query(sql, [obj.Key]);
          if (rows[0].count === 0) {
            unprocessedObjects.push(obj);
          }
        }
      } catch (error) {
        console.error('Error:', error);
      }
    }

    return unprocessedObjects;
  }

  async insertSaveS3File(s3Object) {
    try {
      const sql = `INSERT INTO s3_files (file_key, etag, file_size, last_modified) VALUES (?, ?, ?, ?)`;
      const params = [
        s3Object.Key,
        s3Object.Etag,
        s3Object.Size,
        s3Object.LastModified
      ]
      const result = await this.pool.query(sql, params);
      return result;
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
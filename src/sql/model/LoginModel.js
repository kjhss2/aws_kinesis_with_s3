class LoginModel {
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
  async insertData(record) {
    const sql = `INSERT INTO ${this.tableName} (date, contentType, userId, isNewUser, isReturningUser) 
        VALUES (?, ?, ?, ?, ?)`;
    const params = [
      record.date,
      record.contentType,
      record.userId,
      record.isNewUser,
      record.isReturningUser,
    ]
    const result = await this.pool.query(sql, params);
    return result;
  }

  // MySQL에 데이터를 배치로 삽입하는 함수
  async insertDataBatch(records) {
    // 데이터를 배치로 삽입하는 쿼리
    const sql = `INSERT INTO ${this.tableName} (date, contentType, userId, isNewUser, isReturningUser) VALUES ?`;
    const values = records.map(rowData => {
      const record = JSON.parse(rowData);
      return [
        record.date,
        record.contentType,
        record.userId,
        record.isNewUser,
        record.isReturningUser
      ]
    });

    const [result] = await this.pool.query(sql, [values]);
    console.log(`Inserted ${this.tableName} ${result.affectedRows} rows into MySQL`);
    return result;
  }
}

module.exports = { LoginModel };
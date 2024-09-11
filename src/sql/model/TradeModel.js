class TradeModel {
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

  // MySQL에 데이터를 배치로 삽입하는 함수
  async insertDataBatch(records) {
    // 데이터를 배치로 삽입하는 쿼리
    const sql = `INSERT INTO ${this.tableName} (date, contentType, registerUserId, buyUserId, itemId, paymentType, price, paymentValue, quantity) VALUES ?`;
    const values = records.map(rowData => {
      const record = JSON.parse(rowData);
      return [
        record.date,
        record.contentType,
        record.registerUserId,
        record.buyUserId,
        record.itemId,
        record.paymentType,
        record.price,
        record.paymentValue,
        record.quantity,
      ]
    });

    const [result] = await this.pool.query(sql, [values]);
    console.log(`Inserted ${this.tableName} ${result.affectedRows} rows into MySQL`);
    return result;
  }
}

module.exports = { TradeModel };
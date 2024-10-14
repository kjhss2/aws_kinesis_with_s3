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
  async insertDataBatch(records, batchSize = 1000) {

    let insertCount = 0;

    // 데이터를 배치로 삽입하는 쿼리
    const sql = `INSERT INTO ${this.tableName} (date, log_type, register_user_id, buy_user_id, table_index, item_id, payment_type, price, payment_value, quantity) VALUES ?`;

    for (let i = 0; i < records.length; i += batchSize) {
      const batch = records.slice(i, i + batchSize);
      const values = batch.map(rowData => {
        const record = JSON.parse(rowData);
        return [
          record.date,
          record.logType,
          record.registerUserId,
          record.buyUserId,
          record.tableIndex,
          record.itemId,
          record.paymentType,
          record.price,
          record.paymentValue,
          record.quantity,
        ]
      })

      // 연결 풀에서 연결 가져오기
      const connection = await this.pool.getConnection();

      try {
        const [result] = await connection.query(sql, [values]);
        insertCount += result.affectedRows;
      } finally {
        // 연결 반환
        connection.release();
      }
    }

    return insertCount;
  }
}

module.exports = { TradeModel };
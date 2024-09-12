class ShopModel {
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
    const sql = `INSERT INTO ${this.tableName} (date, contentType, goodsIndex, tableIndex, userId, itemId, paymentType, paymentValue, quantity) VALUES ?`;

    for (let i = 0; i < records.length; i += batchSize) {
      const batch = records.slice(i, i + batchSize);
      const values = batch.map(rowData => {
        const record = JSON.parse(rowData);
        return [
          record.date,
          record.contentType,
          record.goodsIndex,
          record.tableIndex,
          record.userId,
          record.itemId,
          record.paymentType,
          record.paymentValue,
          record.quantity
        ]
      })

      const [result] = await this.pool.query(sql, [values]);
      insertCount += result.affectedRows;
    }

    return insertCount;
  }
}

module.exports = { ShopModel };
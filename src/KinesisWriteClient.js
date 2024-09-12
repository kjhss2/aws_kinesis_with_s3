const { KinesisClient, PutRecordCommand, PutRecordsCommand } = require("@aws-sdk/client-kinesis");

let kinesisInstance;
class KinesisWriteClient {
  constructor() {
    if (kinesisInstance) {
      return kinesisInstance;
    }

    // Kinesis Client
    this.instance = new KinesisClient({
      region: process.env.KINESIS__REGION,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      }
    });
    kinesisInstance = this;
  }

  // Write Kinesis Data
  async writeKinesis(msgData) {
    try {
      const record = JSON.stringify(msgData);
      const recordParams = {
        StreamName: process.env.KINESIS__STREAM_NAME,
        PartitionKey: msgData.tableName,
        Data: Buffer.from(record),
      };
      const response = await this.instance.send(new PutRecordCommand(recordParams));
    } catch (err) {
      console.error("Error sending record to Kinesis:", err);
    }
  }

  async batchWriteKinesis(msgDatas) {
    const BATCH_SIZE = 500; // 한번에 보낼 수 있는 최대 레코드 수
    const batches = [];

    // 500개씩 배치로 나누기
    for (let i = 0; i < msgDatas.length; i += BATCH_SIZE) {
      batches.push(msgDatas.slice(i, i + BATCH_SIZE));
    }

    for (const batch of batches) {
      const recordParams = {
        StreamName: process.env.KINESIS__STREAM_NAME,
        Records: batch.map((record) => {
          return {
            PartitionKey: record.tableName,
            Data: Buffer.from(JSON.stringify(record)),
          }
        })
      };

      try {
        const response = await this.instance.send(new PutRecordsCommand(recordParams));
      } catch (err) {
        console.error("Error sending batch to Kinesis:", err);
      }
    }
  }
}

module.exports = KinesisWriteClient;
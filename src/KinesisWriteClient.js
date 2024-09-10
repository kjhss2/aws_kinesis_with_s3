const { KinesisClient, PutRecordCommand } = require("@aws-sdk/client-kinesis");

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
        PartitionKey: msgData.contentType,
        Data: Buffer.from(record),
      };
      const data = await this.instance.send(new PutRecordCommand(recordParams));
      console.log("Successfully sent record to Kinesis:");
    } catch (err) {
      console.error("Error sending record to Kinesis:", err);
    }
  }
}

module.exports = KinesisWriteClient;
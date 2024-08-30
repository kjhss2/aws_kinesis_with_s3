const { S3Client, GetObjectCommand, ListObjectsV2Command } = require("@aws-sdk/client-s3");
const { leftPadMonth } = require("./commonFunction");

let s3Client = null;
class SaveDBKinesis {
  constructor() {
    if (s3Client) {
      return s3Client;
    }

    // S3 Client
    this.instanse = new S3Client({
      region: process.env.KINESIS__REGION,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      }
    });
    s3Client = this;
  }

  async listObjects(bucket) {
    const params = {
      Bucket: bucket,
    };

    const command = new ListObjectsV2Command(params);

    try {
      const data = await this.instanse.send(command);
      return data.Contents || [];
    } catch (error) {
      console.error("Error listing objects:", error);
      return [];
    }
  }

  async getObject(bucket, key) {
    const params = {
      Bucket: bucket,
      Key: key,
    };

    const command = new GetObjectCommand(params);

    try {
      const response = await this.instanse.send(command);
      const streamToString = (stream) =>
        new Promise((resolve, reject) => {
          const chunks = [];
          stream.on("data", (chunk) => chunks.push(chunk));
          stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
          stream.on("error", reject);
        });

      const objectData = await streamToString(response.Body);
      return objectData;
    } catch (error) {
      console.error(`Error fetching object ${key}:`, error);
      return null;
    }
  }

  async processAllObjects(databaseClient) {
    const bucketName = process.env.KINESIS__S3_BUCKET_NAME;
    const objects = await this.listObjects(bucketName);

    // 어제 날짜의 데이터만 조회
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const eveDate = `${yesterday.getFullYear()}/${leftPadMonth(yesterday.getMonth() + 1)}/${yesterday.getDate()}`;

    for (const object of objects) {
      const key = object.Key;
      if (key && key.includes(eveDate)) {
        const objectData = await this.getObject(bucketName, key);
        if (objectData) {
          const rowDatas = objectData.split("\n");
          for (const rowData of rowDatas) {
            const { tableName, ...data } = JSON.parse(rowData);
            await databaseClient.insertData(tableName, data);
          }
        } else {
          console.error('@objectData is null');
        }
      }
    }
  };
}

module.exports = SaveDBKinesis;
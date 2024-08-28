const { S3Client, GetObjectCommand, ListObjectsV2Command } = require("@aws-sdk/client-s3");
const { leftPad } = require("./commonFunction");
const { MySQL2DBC } = require("./mysql2DBC");

let s3Client = null;
class SaveDBKinesis {
  constructor() {
    // S3 Client
    s3Client = new S3Client({
      region: process.env.KINESIS__REGION,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      }
    });
  }

  async listObjects(bucket) {
    const params = {
      Bucket: bucket,
    };

    const command = new ListObjectsV2Command(params);

    try {
      const data = await s3Client.send(command);
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
      const response = await s3Client.send(command);
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

  async processAllObjects() {

    let databaseClient;

    try {

      const bucketName = process.env.KINESIS__S3_BUCKET_NAME;
      const objects = await this.listObjects(bucketName);
      databaseClient = new MySQL2DBC();

      // 어제 날짜의 데이터만 조회
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);
      const eveDate = `${yesterday.getFullYear()}/${leftPad(yesterday.getMonth() + 1)}/${yesterday.getDate()}`;

      for (const object of objects) {
        const key = object.Key;
        if (key && key.includes(eveDate)) {
          const objectData = await this.getObject(bucketName, key);
          const rowDatas = objectData.split("\n");

          for (const rowData of rowDatas) {
            const { actionToken, ...data } = JSON.parse(rowData);
            databaseClient.insertData(actionToken, data);
          }
        }
      }

    } catch (error) {
      console.error(error);
    } finally {
      if (databaseClient) {
        databaseClient.close();
      }
    }
  };
}

module.exports = SaveDBKinesis;
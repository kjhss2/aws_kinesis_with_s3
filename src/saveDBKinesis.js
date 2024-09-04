const { S3Client, GetObjectCommand, ListObjectsV2Command } = require("@aws-sdk/client-s3");
const { leftPadMonth } = require("./commonFunction");

let s3Client = null;
const saveS3files = [];
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

  async listObjects(bucket, prefixDate) {
    const params = {
      Bucket: bucket,
      Prefix: "logs/" + prefixDate + "/",
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
    // 해당 날짜의 데이터 조회
    const targetDate = new Date();
    targetDate.setDate(targetDate.getDate());
    const readDate = `${targetDate.getFullYear()}/${leftPadMonth(targetDate.getMonth() + 1)}/${leftPadMonth(targetDate.getDate())}`;
    const s3Objects = await this.listObjects(bucketName, readDate);

    for (const s3Object of s3Objects) {
      const key = s3Object.Key;

      // 조건에 맞는 S3 파일인지 체크
      if (key && key.includes(readDate)) {

        // DB에 저장한 S3 파일인지 체크
        if (saveS3files.includes(key)) {
          console.log("@이미 저장된 S3파일");
        } else {
          const objectData = await this.getObject(bucketName, key);
          const rowDatas = objectData.split("\n");

          // Mysql 1건씩 Insert
          // for (const rowData of rowDatas) {
          //   const data = JSON.parse(rowData);
          //   await databaseClient.insertData(data);
          // }

          // Mysql 배치 Insert
          const tableName = JSON.parse(rowDatas?.[0])?.msg?.tableName;
          await databaseClient.insertDataBatch(rowDatas, tableName);

          // DB에 저장한 S3 파일 목록 추가
          saveS3files.push(key);
        }
      }
    }
  };
}

module.exports = SaveDBKinesis;
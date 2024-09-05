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

  async listObjects(continuationToken = null, bucket, prefixDate) {
    const params = {
      Bucket: bucket,
      Prefix: "logs/" + prefixDate + "/",
      MaxKeys: process.env.AWS_S3_MAXKEYS_COUNT,
    };

    if (continuationToken) {
      params.ContinuationToken = continuationToken;
    }

    try {
      const command = new ListObjectsV2Command(params);
      const data = await this.instanse.send(command);
      return {
        s3Objects: data.Contents || [],
        isTruncated: data.IsTruncated,
        nextContinuationToken: data.NextContinuationToken,
      }
    } catch (error) {
      console.error("Error listing s3Objects:", error);
      throw error;
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
    let isTruncated = true;
    let continuationToken = null;

    // 해당 날짜의 데이터 조회
    const targetDate = new Date();
    targetDate.setDate(targetDate.getDate());
    const readDate = `${targetDate.getFullYear()}/${leftPadMonth(targetDate.getMonth() + 1)}/${leftPadMonth(targetDate.getDate())}`;

    while (isTruncated) {
      try {
        // Step 1 : Get AWS S3 file list
        const { s3Objects, isTruncated: newIsTruncated, nextContinuationToken } = await this.listObjects(continuationToken, bucketName, readDate);

        // Step 2 : 이미 DB에 Insert된 S3 파일인지 체크
        const insertS3Objects = await databaseClient.checkSaveS3File(s3Objects);
        if (Array.isArray(insertS3Objects)) {
          // Mysql DB Insert
          for (const s3Object of insertS3Objects) {
            const key = s3Object.Key;

            // Mysql 배치 Insert
            const objectData = await this.getObject(bucketName, key);
            const rowDatas = objectData.split("\n");
            const tableName = JSON.parse(rowDatas?.[0])?.msg?.tableName;
            await databaseClient.insertDataBatch(rowDatas, tableName);

            // DB에 S3 file insert
            await databaseClient.insertSaveS3File(s3Object);
          }
        }

        // Step 3 :페이지네이션 처리
        isTruncated = newIsTruncated;
        continuationToken = nextContinuationToken;

      } catch (error) {
        console.error('Error processing batch:', error);
        break;  // 에러가 발생하면 반복 중단
      }
    }
  };
}

module.exports = SaveDBKinesis;
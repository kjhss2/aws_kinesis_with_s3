const { S3Client, GetObjectCommand, ListObjectsV2Command } = require("@aws-sdk/client-s3");
const dateformat = require('dateformat');
const Enum = require('enum');

const STLOG_TABLES = new Enum({
  STLOG_LOGIN_ACTION: 0,
  STLOG_ITEM_ACTION: 1,
  STLOG_SHOP_ACTION: 2,
  STLOG_TRADE_ACTION: 3,
})

/**
    1 << 0 -> 1
    1 << 1 -> 2
    1 << 2 -> 4
    1 << 3 -> 8
    -----------
             15 = MASK
             local = 0
*/

let s3Client = null;
let saveCompleteBit = '0000';

class S3FileClient {
  constructor() {
    // 인스턴스 중복 생성 방지
    if (s3Client) {
      return s3Client;
    }

    // S3 Client
    this.instanse = new S3Client({
      region: process.env.STLOG_S3_REGION,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      }
    });
    s3Client = this;
    // 내부 변수
    this._isRunnigProcess = false;
  }

  setSaveCompleteBit(position, value) {
    // 문자열을 배열로 변환하여 수정 가능하게 만듦
    const bitArray = saveCompleteBit.split('');

    // 배열에서 해당 위치의 비트를 변경
    bitArray[bitArray.length - 1 - position] = value ? '1' : '0';

    // 배열을 다시 문자열로 변환하여 반환
    saveCompleteBit = bitArray.join('');
  }

  checkSaveCompleteBit() {
    let checkBit = false;
    for (const table of STLOG_TABLES) {
      // bitString을 정수로 변환 (2진수로 파싱)
      const bitValue = parseInt(saveCompleteBit, 2);

      // position에 해당하는 비트를 체크하기 위한 마스크를 생성
      const mask = 1 << table.value;

      // 해당 비트가 활성화되었는지 체크
      checkBit = (bitValue & mask) !== 0;
    }
    return checkBit;
  }

  async listObjects(continuationToken = null, bucket, tableName, prefixDate, prefix_hour) {
    const params = {
      Bucket: bucket,
      Prefix: process.env.STLOG_NAMESPACE + "/logs/" + `${tableName}/` + prefixDate + "/",
      MaxKeys: process.env.STLOG_S3_MAXKEYS_COUNT,
    };

    if (continuationToken) {
      params.ContinuationToken = continuationToken;
    }

    try {
      const command = new ListObjectsV2Command(params);
      const response = await this.instanse.send(command);
      const filteredObjects = [];

      response?.Contents?.forEach((object) => {
        if (object.Size === 0) {
          return false;
        }
        // Extract the hour part (last section of the key) as a number
        const prefixHour = parseInt(object.Key.split('/')[6], 10);
        if (prefixHour >= prefix_hour) {
          filteredObjects.push({ ...object, prefixDate, prefixHour })
        }
      });

      return {
        s3Objects: filteredObjects || [],
        isTruncated: response.IsTruncated,
        nextContinuationToken: response.NextContinuationToken,
      }
    } catch (error) {
      console.error(`Error listing s3Objects:", ${error}`);
      throw error;
    }
  }

  async getObjectAndDBInsert(bucketName, databaseClient, tableObject, insertS3Objects) {
    try {
      // running saveCompleteBit
      this.setSaveCompleteBit(tableObject.value, true);
      let insertDBCount = 0;
      console.info(`Start tableName = ${tableObject.key}`);
      console.time(`stlog-manager-job] ${tableObject.key}`);

      for (const s3Object of insertS3Objects) {
        // Step 1-1 : load S3 object
        const params = {
          Bucket: bucketName,
          Key: s3Object.Key,
        };
        const command = new GetObjectCommand(params);
        const response = await this.instanse.send(command);
        const streamToString = (stream) =>
          new Promise((resolve, reject) => {
            const chunks = [];
            stream.on("data", (chunk) => chunks.push(chunk));
            stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
            stream.on("error", reject);
          });
        const objectData = await streamToString(response.Body);

        // Step 1-2 : parse data
        const rowDatas = objectData.split("\n");

        // Step 2-1 : DB data Insert
        const affectedRows = await databaseClient.insertDataBatch(rowDatas, tableObject.key);
        insertDBCount += affectedRows || 0;

        // Step 2-2 : DB S3 corsor insert
        await databaseClient.upsertS3FileCursor(tableObject.key, s3Object);
      }

      console.timeEnd(`stlog-manager-job] ${tableObject.key}`);
      console.info(`Complete tableName = ${tableObject.key} / insertRow = ${insertDBCount}`);
    } catch (error) {
      console.error(`getObjectAndDBInsert fetching ${error}`);
      return null;
    } finally {
      // complete bit
      this.setSaveCompleteBit(tableObject.value, false);
    }
  }

  async processAllObjects(databaseClient) {
    // 이미 프로세스가 진행 중 이라면 진행 하지 않음
    if (this.checkSaveCompleteBit() === true) {
      console.info(`already DB Insert process running`);
      return;
    }

    const bucketName = process.env.STLOG_S3_BUCKET_NAME;

    // stlog table for-of
    for (const tableObject of STLOG_TABLES) {
      let isTruncated = true;
      let continuationToken = null;

      // 해당 날짜의 데이터 조회
      const targetDate = new Date();
      // targetDate.setDate(targetDate.getDate());
      const prefixDate = `${targetDate.getFullYear()}/${dateformat(targetDate, 'mm')}/${dateformat(targetDate, 'dd')}`;

      while (isTruncated) {
        try {
          const cursorRow = await databaseClient.getS3FileCursor(tableObject.key, prefixDate);

          // Step 1 : Get AWS S3 file list
          const { s3Objects, isTruncated: newIsTruncated, nextContinuationToken } = await this.listObjects(
            continuationToken,
            bucketName,
            tableObject.key,
            prefixDate,
            cursorRow?.prefix_hour
          );

          // Step 2 : 이미 DB에 Insert된 S3 파일 인지 체크
          const insertS3Objects = await databaseClient.makeInsertS3Objects(cursorRow?.file_key, s3Objects);

          // Step 3 : 페이지네이션 처리
          isTruncated = newIsTruncated;
          continuationToken = nextContinuationToken;

          // Step 4 : get S3 Oobject and DB Insert
          if (Array.isArray(insertS3Objects) && insertS3Objects.length > 0) {
            this.getObjectAndDBInsert(bucketName, databaseClient, tableObject, insertS3Objects)
          }
        } catch (error) {
          console.error(`processAllObjects processing batch: ${error}`);
        }
      }
    }
  }
}

module.exports = S3FileClient;
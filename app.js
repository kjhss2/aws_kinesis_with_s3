const http = require('http');
const schedule = require('node-schedule');
const S3FileClient = require('./src/S3FileClient');
const { MySQL2DBC } = require('./src/sql/Mysql2DBC');
const kinesisWriteTest = require('./src/lib/kinesisWriteTest');

const result = require('dotenv').config({ path: __dirname + `/default.env` });
if (result.error) {
  console.log(`[default.env] laod failed. ${result.error}`);
  process.exit(1);
}

// http.createServer((request, response) => {
//   response.statusCode = 200;
//   response.setHeader('Content-Type', 'text/plain');
//   response.end('Hello world');
// }).listen(4000);

// Write Kinesis : 더미 데이터 Write(0 0/1 * * * * 10분 마다)
const writeDummyKinesisJob = schedule.scheduleJob('0 0/1 * * * *', function () {
  if (Number(process.env.KINESIS__WRITE_DUMMY_ACTIVE) === 1) {
    // 속도 측정(Start)
    console.time("writeDummyKinesisJob");
    kinesisWriteTest.makeDummyLoginData();
    kinesisWriteTest.makeDummyLogoutData();
    kinesisWriteTest.makeDummyShopBuyData();
    kinesisWriteTest.makeDummyShopSellData();
    kinesisWriteTest.makeDummyTradeRegisterData();
    kinesisWriteTest.makeDummyTradeBuyData();
    // 속도 측정(End)
    console.timeEnd("writeDummyKinesisJob");
  }
})

// Run Save MySQL 스케쥴 : 매일 오전 1시에 실행(0 0/1 * * * * 1분 마다)
const saveMysqlJob = schedule.scheduleJob('0 0/1 * * * *', async () => {
  if (Number(process.env.KINESIS__INSERT_DB_ACTIVE) === 1) {
    let databaseClient = null;

    try {
      databaseClient = new MySQL2DBC();
      const s3FileClient = new S3FileClient();
      await s3FileClient.processAllObjects(databaseClient);
    } catch (error) {
      console.error('Job Error:', error);
    } finally {
      if (databaseClient) {
        await databaseClient.close();
      }
    }
  }
});

console.log('Server running at http://127.0.0.1:4000');
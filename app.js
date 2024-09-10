const http = require('http');
const schedule = require('node-schedule');
const KinesisWriteClient = require('./src/KinesisWriteClient');
const S3FileClient = require('./src/S3FileClient');
const { MySQL2DBC } = require('./src/sql/Mysql2DBC');

const result = require('dotenv').config({ path: __dirname + `/default.env` });
if (result.error) {
  console.log(`[default.env] laod failed. ${result.error}`);
  process.exit(1);
}

http.createServer((request, response) => {
  response.statusCode = 200;
  response.setHeader('Content-Type', 'text/plain');
  response.end('Hello world');
}).listen(4000);

// Write Kinesis : 더미 데이터 Write(0 0/1 * * * * 10분 마다)
const writeKinesisJob = schedule.scheduleJob('0 0/10 * * * *', function () {
  const kinesisWriteClient = new KinesisWriteClient();

  // Test Input Data
  const msgDatas = [
    {
      contentType: "LOGIN", nowDate: new Date(), base: '', msg: {
        tableName: "DE_LOGIN_ACTION",
        value: Math.random() * 100
      }
    },
    {
      contentType: "SHOP", nowDate: new Date(), base: '', msg: {
        tableName: "DE_SERVER_ACTION",
        value: Math.random() * 100
      }
    },
    {
      contentType: "RANK", nowDate: new Date(), base: '', msg: {
        tableName: "DE_DS_ACTION",
        value: Math.random() * 100
      }
    },
  ];

  msgDatas.forEach(data => {
    kinesisWriteClient.writeKinesis(data);
  });
})

// Run Save MySQL 스케쥴 : 매일 오전 1시에 실행(0 0/1 * * * * 1분 마다)
const saveMysqlJob = schedule.scheduleJob('0 0/1 * * * *', async () => {
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
});

console.log('Server running at http://127.0.0.1:4000');
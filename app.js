const http = require('http');
const schedule = require('node-schedule');
const KinesisClientClass = require('./src/kinesis-client');
const SaveDBKinesis = require('./src/saveDBKinesis');
const { MySQL2DBC } = require('./src/mysql2DBC');

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
  const kinesisClientClass = new KinesisClientClass();

  // Test Input Data
  const msgDatas = [
    { actionToken: "DE_LOGIN_ACTION", date: new Date(), type: 'MSG', value: Math.random() * 100 },
    { actionToken: "DE_SERVER_ACTION", date: new Date(), type: 'MSG', value: Math.random() * 100 },
    { actionToken: "DE_DS_ACTION", date: new Date(), type: 'MSG', value: Math.random() * 100 },
  ];

  msgDatas.forEach(data => {
    kinesisClientClass.writeKinesis(data);
  });
})

// Run Save MySQL 스케쥴 : 매일 오전 1시에 실행(0 0/1 * * * * 1분 마다)
const saveMysqlJob = schedule.scheduleJob('0 0 1 * * *', async () => {
  let databaseClient = null;

  try {
    databaseClient = new MySQL2DBC();
    databaseClient.getDataById(1);

    const saveDBKinesis = new SaveDBKinesis();
    await saveDBKinesis.processAllObjects(databaseClient);
  } catch (error) {
    console.error('Job Error:', error);
  } finally {
    if (databaseClient) {
      await databaseClient.close();
    }
  }
});

console.log('Server running at http://127.0.0.1:4000');
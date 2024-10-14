const schedule = require('node-schedule');
const S3FileClient = require('./src/s3-file-client');
const writeKinesisTest = require('./src/write/write-kinesis-test');
const { MySQL2DBC } = require('./src/sql/mysql2dbc');

const result = require('dotenv').config({ path: __dirname + `/default.env` });
if (result.error) {
  console.log(`[default.env] laod failed. ${result.error}`);
  process.exit(1);
}

// Write Kinesis : 더미 데이터 Write(0 0/1 * * * * 1분 마다)
schedule.scheduleJob('0 0/1 * * * *', function () {
  if (Number(process.env.KINESIS__WRITE_DUMMY_ACTIVE) === 1) {
    // 속도 측정(Start)
    console.time("writeDummyKinesisJob");
    writeKinesisTest.makeDummyLoginData();
    writeKinesisTest.makeDummyLogoutData();
    writeKinesisTest.makeDummyShopBuyData();
    writeKinesisTest.makeDummyShopSellData();
    writeKinesisTest.makeDummyTradeRegisterData();
    writeKinesisTest.makeDummyTradeBuyData();
    writeKinesisTest.makeDummyUpgradeItemData();
    writeKinesisTest.makeDummyRerollItemData();
    // 속도 측정(End)
    console.timeEnd("writeDummyKinesisJob");
  }
})

// Run insert log MySQL 스케쥴 : 0 0/1 * * * * 1분 마다 실행
schedule.scheduleJob('0 0/1 * * * *', async () => {
  let databaseClient = null;
  try {
    databaseClient = new MySQL2DBC();
    const s3FileClient = new S3FileClient();
    await s3FileClient.processAllObjects(databaseClient);
  } catch (error) {
    console.error(`log-manager Job Error : ${error}`);
  }
});

const runSqlMultiPoolJob = () => {
  try {
    const kinesisSQLClient = require('./src/sql-multi-pool/kinesis-mysql-client');
    // kinesis mysql initialize
    if (!kinesisSQLClient.initialize()) {
      console.error('runSqlMultiPoolJob Initialization failed.');
      process.exit(1);
    }

    kinesisSQLClient.test_query();
  } catch (error) {

  } finally {

  }
};

console.log('stlog-manager Running');
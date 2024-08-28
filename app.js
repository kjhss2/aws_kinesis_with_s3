const http = require('http');
const KinesisClientClass = require('./src/kinesis-client');
const SaveDBKinesis = require('./src/saveDBKinesis');

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

// Write Kinesis
(() => {
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
})();

// Save MySQL
(() => {
	const saveDBKinesis = new SaveDBKinesis();
	saveDBKinesis.processAllObjects();
})();

console.log('Server running at http://127.0.0.1:4000');
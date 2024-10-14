const KinesisWriteClient = require("./write-kinesis-client");
const { createUUID, getRandomI } = require("../lib/commonFunction");
const Enum = require('enum');

// 더미 데이터 생성 개수
const PUT_DATA_COUNT = 100000;

const StlogType = new Enum({
    // STLOG_LOGIN_ACTION
    LOGIN: 0,
    LOGOUT: 1,
    // STLOG_SHOP_ACTION
    BUY_SHOP_ITEM: 0,
    SELL_SHOP_ITEM: 1,
    // STLOG_TRADE_ACTION
    REGISTER_TRADE_ITEM: 0,
    BUY_TRADE_ITEM: 1,
    // STLOG_ITEM_ACTION
    UPGRADE_ITEM: 0,
    REROLL_ITEM: 1,
});

const makeDummyLoginData = async (tableName = "STLOG_LOGIN_ACTION") => {

    const kinesisWriteClient = new KinesisWriteClient();
    let sendDataCount = PUT_DATA_COUNT;
    const msgDatas = [];

    while (sendDataCount > 0) {
        sendDataCount--;
        const msgData = {
            tableName,
            logType: StlogType.LOGIN.value,
            userId: createUUID(),
            ip: '127.0.0.1',
            isNewUser: false,
        };
        msgDatas.push(msgData);
    }

    await kinesisWriteClient.batchWriteKinesis(msgDatas);
}

const makeDummyLogoutData = async (tableName = "STLOG_LOGIN_ACTION") => {

    const kinesisWriteClient = new KinesisWriteClient();
    let sendDataCount = PUT_DATA_COUNT;
    const msgDatas = [];

    while (sendDataCount > 0) {
        sendDataCount--;
        const msgData = {
            tableName,
            logType: StlogType.LOGOUT.value,
            userId: createUUID(),
            ip: '127.0.0.1',
            isNewUser: false,
        };
        msgDatas.push(msgData);
    }

    await kinesisWriteClient.batchWriteKinesis(msgDatas);
}

const makeDummyShopBuyData = async (tableName = "STLOG_SHOP_ACTION") => {

    const kinesisWriteClient = new KinesisWriteClient();
    let sendDataCount = PUT_DATA_COUNT;
    const msgDatas = [];

    while (sendDataCount > 0) {
        sendDataCount--;
        const msgData = {
            tableName,
            logType: StlogType.BUY_SHOP_ITEM.value,
            tableIndex: getRandomI(),
            userId: createUUID(),
            goodsIndex: getRandomI(),
            paymentType: '1',
            paymentValue: getRandomI(),
            quantity: getRandomI(),
        };
        msgDatas.push(msgData);
    }

    await kinesisWriteClient.batchWriteKinesis(msgDatas);
}

const makeDummyShopSellData = async (tableName = "STLOG_SHOP_ACTION") => {

    const kinesisWriteClient = new KinesisWriteClient();
    let sendDataCount = PUT_DATA_COUNT;
    const msgDatas = [];

    while (sendDataCount > 0) {
        sendDataCount--;
        const msgData = {
            tableName,
            logType: StlogType.SELL_SHOP_ITEM.value,
            userId: createUUID(),
            itemId: createUUID(),
            tableIndex: getRandomI(),
            paymentType: '1',
            paymentValue: getRandomI(),
            quantity: getRandomI(),
        };
        msgDatas.push(msgData);
    }

    await kinesisWriteClient.batchWriteKinesis(msgDatas);
}

const makeDummyTradeRegisterData = async (tableName = "STLOG_TRADE_ACTION") => {

    const kinesisWriteClient = new KinesisWriteClient();
    let sendDataCount = PUT_DATA_COUNT;
    const msgDatas = [];

    while (sendDataCount > 0) {
        sendDataCount--;
        const msgData = {
            tableName,
            logType: StlogType.REGISTER_TRADE_ITEM.value,
            registerUserId: createUUID(),
            tableIndex: getRandomI(),
            itemId: createUUID(),
            paymentType: '1',
            price: getRandomI(),
            paymentValue: getRandomI(),
            quantity: getRandomI(),
        };
        msgDatas.push(msgData);
    }

    await kinesisWriteClient.batchWriteKinesis(msgDatas);
}

const makeDummyTradeBuyData = async (tableName = "STLOG_TRADE_ACTION") => {

    const kinesisWriteClient = new KinesisWriteClient();
    let sendDataCount = PUT_DATA_COUNT;
    const msgDatas = [];

    while (sendDataCount > 0) {
        sendDataCount--;
        const msgData = {
            tableName,
            logType: StlogType.BUY_TRADE_ITEM.value,
            registerUserId: createUUID(),
            buyUserId: createUUID(),
            tableIndex: getRandomI(),
            itemId: createUUID(),
            paymentType: '1',
            price: getRandomI(),
            paymentValue: getRandomI(),
            quantity: getRandomI(),
        };
        msgDatas.push(msgData);
    }

    await kinesisWriteClient.batchWriteKinesis(msgDatas);
}

const makeDummyUpgradeItemData = async (tableName = "STLOG_ITEM_ACTION") => {

    const kinesisWriteClient = new KinesisWriteClient();
    let sendDataCount = PUT_DATA_COUNT;
    const msgDatas = [];

    while (sendDataCount > 0) {
        sendDataCount--;
        const delta = getRandomI(0, 1);
        const msgData = {
            tableName,
            logType: StlogType.UPGRADE_ITEM.value,
            userId: createUUID(),
            tableIndex: getRandomI(),
            itemId: createUUID(),
            delta: delta,
            before: '',
            after: '',
        };
        msgDatas.push(msgData);
    }

    await kinesisWriteClient.batchWriteKinesis(msgDatas);
}

const makeDummyRerollItemData = async (tableName = "STLOG_ITEM_ACTION") => {

    const kinesisWriteClient = new KinesisWriteClient();
    let sendDataCount = PUT_DATA_COUNT;
    const msgDatas = [];

    while (sendDataCount > 0) {
        sendDataCount--;
        const delta = getRandomI(0, 1);
        const msgData = {
            tableName,
            logType: StlogType.REROLL_ITEM.value,
            userId: createUUID(),
            tableIndex: getRandomI(),
            itemId: createUUID(),
            delta: delta,
            before: '',
            after: '',
        };
        msgDatas.push(msgData);
    }

    await kinesisWriteClient.batchWriteKinesis(msgDatas);
}

module.exports = {
    makeDummyLoginData,
    makeDummyLogoutData,
    makeDummyShopBuyData,
    makeDummyShopSellData,
    makeDummyTradeRegisterData,
    makeDummyTradeBuyData,
    makeDummyUpgradeItemData,
    makeDummyRerollItemData
};
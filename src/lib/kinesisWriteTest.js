const dateFormat = require('dateformat');
const KinesisWriteClient = require("../KinesisWriteClient");
const { createUUID, getRandomI } = require("./commonFunction");

// 더미 데이터 생성 개수
const PUT_DATA_COUNT = 100000;

const makeDummyLoginData = async (tableName = "DE_LOGIN_ACTION") => {

    const kinesisWriteClient = new KinesisWriteClient();
    let sendDataCount = PUT_DATA_COUNT;
    const msgDatas = [];

    while (sendDataCount > 0) {
        sendDataCount--;
        const nowDate = Date.now();
        const msgData = {
            tableName,
            date: dateFormat(nowDate, "UTC:yyyy-mm-dd HH:MM:ss"),
            contentType: 'LOGIN',
            userId: createUUID(),
            isNewUser: false,
            isReturningUser: false,
        };
        msgDatas.push(msgData);
    }

    await kinesisWriteClient.batchWriteKinesis(msgDatas);
}

const makeDummyLogoutData = async (tableName = "DE_LOGIN_ACTION") => {

    const kinesisWriteClient = new KinesisWriteClient();
    let sendDataCount = PUT_DATA_COUNT;
    const msgDatas = [];

    while (sendDataCount > 0) {
        sendDataCount--;
        const nowDate = Date.now();
        const msgData = {
            tableName,
            date: dateFormat(nowDate, "UTC:yyyy-mm-dd HH:MM:ss"),
            contentType: 'LOGOUT',
            userId: createUUID(),
            isNewUser: false,
            isReturningUser: false,
        };
        msgDatas.push(msgData);
    }

    await kinesisWriteClient.batchWriteKinesis(msgDatas);
}

const makeDummyShopBuyData = async (tableName = "DE_SHOP_ACTION") => {

    const kinesisWriteClient = new KinesisWriteClient();
    let sendDataCount = PUT_DATA_COUNT;
    const msgDatas = [];

    while (sendDataCount > 0) {
        sendDataCount--;
        const nowDate = Date.now();
        const msgData = {
            tableName,
            date: dateFormat(nowDate, "UTC:yyyy-mm-dd HH:MM:ss"),
            contentType: 'BUY_SHOP_ITEM',
            tableIndex: getRandomI(),
            userId: createUUID(),
            goodsIndex: getRandomI(),
            paymentType: 'By_Gold',
            paymentValue: getRandomI(),
            quantity: getRandomI(),
        };
        msgDatas.push(msgData);
    }

    await kinesisWriteClient.batchWriteKinesis(msgDatas);
}

const makeDummyShopSellData = async (tableName = "DE_SHOP_ACTION") => {

    const kinesisWriteClient = new KinesisWriteClient();
    let sendDataCount = PUT_DATA_COUNT;
    const msgDatas = [];

    while (sendDataCount > 0) {
        sendDataCount--;
        const nowDate = Date.now();
        const msgData = {
            tableName,
            date: dateFormat(nowDate, "UTC:yyyy-mm-dd HH:MM:ss"),
            contentType: 'SELL_SHOP_ITEM',
            userId: createUUID(),
            itemId: createUUID(),
            tableIndex: getRandomI(),
            paymentType: 'By_Gold',
            paymentValue: getRandomI(),
            quantity: getRandomI(),
        };
        msgDatas.push(msgData);
    }

    await kinesisWriteClient.batchWriteKinesis(msgDatas);
}

const makeDummyTradeRegisterData = async (tableName = "DE_TRADE_ACTION") => {

    const kinesisWriteClient = new KinesisWriteClient();
    let sendDataCount = PUT_DATA_COUNT;
    const msgDatas = [];

    while (sendDataCount > 0) {
        sendDataCount--;
        const nowDate = Date.now();
        const msgData = {
            tableName,
            date: dateFormat(nowDate, "UTC:yyyy-mm-dd HH:MM:ss"),
            contentType: 'REGISTER_TRADE_ITEM',
            registerUserId: createUUID(),
            tableIndex: getRandomI(),
            itemId: createUUID(),
            paymentType: 'By_Gold',
            price: getRandomI(),
            paymentValue: getRandomI(),
            quantity: getRandomI(),
        };
        msgDatas.push(msgData);
    }

    await kinesisWriteClient.batchWriteKinesis(msgDatas);
}

const makeDummyTradeBuyData = async (tableName = "DE_TRADE_ACTION") => {

    const kinesisWriteClient = new KinesisWriteClient();
    let sendDataCount = PUT_DATA_COUNT;
    const msgDatas = [];

    while (sendDataCount > 0) {
        sendDataCount--;
        const nowDate = Date.now();
        const msgData = {
            tableName,
            date: dateFormat(nowDate, "UTC:yyyy-mm-dd HH:MM:ss"),
            contentType: 'BUY_TRADE_ITEM',
            registerUserId: createUUID(),
            buyUserId: createUUID(),
            tableIndex: getRandomI(),
            itemId: createUUID(),
            paymentType: 'By_Gold',
            price: getRandomI(),
            paymentValue: getRandomI(),
            quantity: getRandomI(),
        };
        msgDatas.push(msgData);
    }

    await kinesisWriteClient.batchWriteKinesis(msgDatas);
}

const makeDummyUpgradeItemData = async (tableName = "DE_ITEM_ACTION") => {

    const kinesisWriteClient = new KinesisWriteClient();
    let sendDataCount = PUT_DATA_COUNT;
    const msgDatas = [];

    while (sendDataCount > 0) {
        sendDataCount--;
        const nowDate = Date.now();
        const delta = getRandomI(0, 1);
        const msgData = {
            tableName,
            date: dateFormat(nowDate, "UTC:yyyy-mm-dd HH:MM:ss"),
            contentType: 'UPGRADE_ITEM',
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

const makeDummyRerollItemData = async (tableName = "DE_ITEM_ACTION") => {

    const kinesisWriteClient = new KinesisWriteClient();
    let sendDataCount = PUT_DATA_COUNT;
    const msgDatas = [];

    while (sendDataCount > 0) {
        sendDataCount--;
        const nowDate = Date.now();
        const msgData = {
            tableName,
            date: dateFormat(nowDate, "UTC:yyyy-mm-dd HH:MM:ss"),
            contentType: 'REROLL_ITEM',
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
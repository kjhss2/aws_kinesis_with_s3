const dateFormat = require('dateformat');
const KinesisWriteClient = require("../KinesisWriteClient");
const { createUUID, getRandomI } = require("./commonFunction");

// 더미 데이터 생성 개수
const PUT_DATA_COUNT = 100000;

const makeDummyLoginData = async (tableName = "DE_LOGIN_ACTION") => {

    let sendDataCount = PUT_DATA_COUNT;
    const kinesisWriteClient = new KinesisWriteClient();

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
        await kinesisWriteClient.writeKinesis(msgData);
    }
}

const makeDummyLogoutData = async (tableName = "DE_LOGIN_ACTION") => {

    let sendDataCount = PUT_DATA_COUNT;
    const kinesisWriteClient = new KinesisWriteClient();

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
        await kinesisWriteClient.writeKinesis(msgData);
    }
}

const makeDummyShopBuyData = async (tableName = "DE_SHOP_ACTION") => {

    let sendDataCount = PUT_DATA_COUNT;
    const kinesisWriteClient = new KinesisWriteClient();

    while (sendDataCount > 0) {
        sendDataCount--;
        const nowDate = Date.now();
        const msgData = {
            tableName,
            date: dateFormat(nowDate, "UTC:yyyy-mm-dd HH:MM:ss"),
            contentType: 'BUY_SHOP_ITEM',
            userId: createUUID(),
            goodsIndex: getRandomI(),
            paymentType: 'By_Gold',
            paymentValue: getRandomI(),
            quantity: getRandomI(),
        };
        await kinesisWriteClient.writeKinesis(msgData);
    }
}

const makeDummyShopSellData = async (tableName = "DE_SHOP_ACTION") => {

    let sendDataCount = PUT_DATA_COUNT;
    const kinesisWriteClient = new KinesisWriteClient();

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
        await kinesisWriteClient.writeKinesis(msgData);
    }
}

const makeDummyTradeRegisterData = async (tableName = "DE_TRADE_ACTION") => {

    let sendDataCount = PUT_DATA_COUNT;
    const kinesisWriteClient = new KinesisWriteClient();

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
        await kinesisWriteClient.writeKinesis(msgData);
    }
}

const makeDummyTradeBuyData = async (tableName = "DE_TRADE_ACTION") => {

    let sendDataCount = PUT_DATA_COUNT;
    const kinesisWriteClient = new KinesisWriteClient();

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
        await kinesisWriteClient.writeKinesis(msgData);
    }
}

module.exports = {
    makeDummyLoginData,
    makeDummyLogoutData,
    makeDummyShopBuyData,
    makeDummyShopSellData,
    makeDummyTradeRegisterData,
    makeDummyTradeBuyData
};
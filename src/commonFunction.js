function leftPad(value) {
    if (value >= 10) {
        return value;
    }

    return `0${value}`;
}

module.exports = { leftPad };
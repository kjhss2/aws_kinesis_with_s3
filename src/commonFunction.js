const U = module.exports;

U.leftPadMonth = function (value) {
  if (value >= 10) {
    return value;
  }

  return `0${value}`;
}

U.getRandomI = function (min, max) {

  if (min == null || min == undefined) throw new Error('getRandomI: invalid number');

  // 1. max 가 없다면 [0, min) 사이의 정수 반환
  if (max == null || max == undefined) {
    return Math.floor(U.random() * Math.floor(min));
  }

  if (min > max) throw new Error(`getRandomI: min(${min}) > max(${max})`);

  // 2. min, max 가 유효한 숫자이면 [min, max] 사이의 정수 반환
  min = Math.floor(min);
  max = Math.floor(max);

  return Math.floor(U.random() * (max - min + 1)) + min;
}

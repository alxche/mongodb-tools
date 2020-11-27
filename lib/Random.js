/** @format */

const _ = require('lodash')

class Random {
  id(charsCount = 20) {
    return this._randomString(charsCount, '23456789ABCDEFGHJKLMNPQRSTWXYZabcdefghijkmnopqrstuvwxyz')
  }

  hex(digits = 32) {
    return this._randomString(digits, '0123456789abcdef')
  }

  secret(charsCount = 43) {
    return this._randomString(charsCount, 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_')
  }

  choice(arrayOrString) {
    const index = Math.floor(this.fraction() * arrayOrString.length)
    if (typeof arrayOrString === 'string') {
      return arrayOrString.substr(index, 1)
    }
    return arrayOrString[index]
  }

  _randomString(charsCount, alphabet) {
    const digits = []
    for (let i = 0; i < charsCount; i++) {
      digits[i] = this.choice(alphabet)
    }
    return digits.join('')
  }

  fraction() {
    return _.random(1, true)
  }
}

module.exports = new Random()

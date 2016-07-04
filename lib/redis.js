var Promise     = require('bluebird')
  , winston     = require('winston')
  , config      = require('config')
  , _           = require('lodash')
  , redis       = require('redis')
  ;

Promise.promisifyAll(redis.RedisClient.prototype);
Promise.promisifyAll(redis.Multi.prototype);

function connect() {
  return redis.createClient({ db : config.get('cron.redis.db') });
}

module.exports = {
    connect : connect
};
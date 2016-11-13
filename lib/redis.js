var Promise     = require('bluebird')
  , winston     = require('winston')
  , config      = require('config')
  , _           = require('lodash')
  , redis       = require('redis')
  ;

Promise.promisifyAll(redis.RedisClient.prototype);
Promise.promisifyAll(redis.Multi.prototype);

function connect() {
  return redis.createClient({ 
      db        : config.get('cron.redis.db')
    , host      : config.get('cron.redis.host')
    , port      : config.get('cron.redis.port')
    , password  : config.get('cron.redis.password')
  });
}

module.exports = {
    connect : connect
};
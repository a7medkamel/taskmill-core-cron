var Promise     = require('bluebird')
  , winston     = require('winston')
  , config      = require('config')
  , _           = require('lodash')
  , redis       = require('redis')
  ;

Promise.promisifyAll(redis.RedisClient.prototype);
Promise.promisifyAll(redis.Multi.prototype);

function connect() {
  let opt = { 
      db        : config.get('cron.redis.db')
    , host      : config.get('cron.redis.host')
    , port      : config.get('cron.redis.port')
  };

  if (config.has('cron.redis.password') && config.get('cron.redis.password')) {
    opt.password = config.get('cron.redis.password');
  }

  return redis.createClient(opt);
}

module.exports = {
    connect : connect
};
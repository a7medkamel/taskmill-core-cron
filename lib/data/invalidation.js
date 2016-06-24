var Promise     = require('bluebird')
  , winston     = require('winston')
  , config      = require('config')
  , _           = require('lodash')
  , redis       = require('redis')
  ;

var client = redis.createClient();

client.on('error', function (err) {
  winston.error('redis', err);
});

function to_response(result) {
  return  {
            data  : _.map(result, (item) => {
                      var keys    = item.split('#')
                        , remote  = keys[0]
                        , branch  = keys[1]
                        ;

                      return {
                          remote  : remote
                        , branch  : branch
                      }
                    })
          };
}

function madd(members, cb) {
  if (_.size(members) === 0) {
    return Promise.resolve().asCallback(cb);
  }

  winston.debug('invalidation', 'add', members);
  return Promise
          .fromCallback((cb) => {
            var at  = new Date().getTime()
              , mem = _.map(members, (m) => m.remote + '#' + m.branch)
              , ats = _.times(_.size(mem), _.constant(at))
              , arg = _.concat(['cron:invalidation'], _.flatten(_.unzip([ats, mem])))
              ;

            client.zadd(arg, cb);
          })
          .catch((err) => {
            winston.error('wtf', err);
          })
          .asCallback(cb);
}

function pending(cb) {
  return Promise
          .fromCallback((cb) => {
            var min = 0
              , max = new Date().getTime()
              ;

            // todo [akamel] make config limit
            client.zrangebyscore([ 'cron:invalidation', min, max, 'LIMIT', 0, config.get('cron.batch.read') ], cb);
          })
          .then((result) => {
            winston.debug('invalidation', 'pending', result);
            return to_response(result);
          })
          .asCallback(cb);
}

function trim(count, cb) {
  if (count === 0) {
    winston.debug('invalidation', 'trim', '[skipped]');
    return Promise.resolve().asCallback(cb);
  }
  
  winston.debug('invalidation', 'trim', count);
  return Promise
          .fromCallback((cb) => {
            client.zremrangebyrank([ 'cron:invalidation', 0, count - 1 ], cb)
          })
          .then((result) => {
            return to_response(result);
          })
          .asCallback(cb);
}

function pull(cb) {
  return pending()
          .tap((response) => {
            var count = _.size(response.data);

            return trim(count);
              // .catch((err) => {
              //   winston.error('invalidation', 'trim', 'background', err);
              // });
          })
          .asCallback(cb);
}

module.exports = {
    madd    : madd
  , pull    : pull
};

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
  var pairs = _.chunk(result, 2);

  return  {
            data  : _.map(pairs, (pair) => {
                      var keys    = pair[0].split('#')
                        , remote  = keys[0]
                        , branch  = keys[1]
                        , score   = pair[1]
                        ;

                      return {
                          remote  : remote
                        , branch  : branch
                        , at      : parseInt(score)
                      }
                    })
          };
}

function pending(cb) {
  return Promise
          .fromCallback((cb) => {
            var min = 0
              , max = new Date().getTime()
              ;

            winston.debug('set', 'pending-check', '@max', max);
            // todo [akamel] make config limit
            client.zrangebyscore([ 'cron', min, max, 'WITHSCORES', 'LIMIT', 0, config.get('cron.batch.read') ], cb);
          })
          .then((result) => {
            winston.debug('set', 'pending', result);
            return to_response(result);
          })
          .asCallback(cb);
}

function peek(cb) {
  return Promise
          .fromCallback((cb) => {
            client.zrange([ 'cron', 0, 99, 'WITHSCORES'], cb);
          })
          .then((result) => {
            return to_response(result);
          })
          .asCallback(cb);
}

function trim(count, cb) {
  if (count === 0) {
    winston.debug('set', 'trim', '[skipped]');
    return Promise.resolve().asCallback(cb);
  }

  winston.debug('set', 'trim', count);
  return Promise
          .fromCallback((cb) => {
            client.zremrangebyrank([ 'cron', 0, count - 1 ], cb)
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

function add(remote, branch, at, cb) {
  winston.debug('set', 'add', remote, branch, at);

  return Promise
          .fromCallback((cb) => {
            client.zadd('cron', at, remote + '#' + branch, cb);
          })
          .asCallback(cb);
}

function madd(members, cb) {
  if (_.size(members) === 0) {
    return Promise.resolve().asCallback(cb);
  }

  var chunks = _.chunk(members, config.get('cron.batch.write'));
  return Promise
          .map(chunks, (members) => {
            var arg = _.concat('cron', _.chain(members).map((m) => [ m.at, m.remote + '#' + m.branch ]).flatten().value());

            return Promise
                    .fromCallback((cb) => {
                      client.zadd(arg, cb);
                    })
                    .tap((ret) => {
                      if (ret === 0) {
                        winston.error('zadd failed', _.size(members), 'chunk');
                        console.log(arg);
                      } else {
                        winston.debug('set', 'madd', ret);
                      }
                    });
          })
          .then((rets) => {
            return _.sum(rets);
          })
          .asCallback(cb);
}

module.exports = {
    pending : pending
  , peek    : peek
  , trim    : trim
  , pull    : pull
  , add     : add
  , madd    : madd
};

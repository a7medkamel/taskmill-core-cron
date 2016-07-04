var Promise       = require('bluebird')
  , winston       = require('winston')
  , _             = require('lodash')
  , config        = require('config-url')
  , async         = require('async')
  , express       = require('express')
  , bodyParser    = require('body-parser')
  , cron          = require('../cron')
  , redis         = require('../redis').connect()
  , sub           = require('../redis').connect()
  ;

winston.level = config.get('cron.winston.level');

redis.on('error', (err) => {
  winston.error('redis', err);
});

sub.on('error', (err) => {
  winston.error('redis', err);
});

var max = 0;

sub.subscribe('cron:cursor');
sub.on('message', (channel, message) => {
    winston.debug(channel, 'done with items scheduled up to', new Date(parseInt(message)).toTimeString());
    max = _.parseInt(message);
});

var cargo = async.cargo((keys, cb) => {
  if (_.size(keys) === 0) {
    _.defer(cb);
  }

  return redis
          .mgetAsync(keys)
          .then((res) => _.map(res, JSON.parse) )
          .map((repo, index) => {
            var after = new Date().getTime() + 1000 // in case getTime is same as current run
              , at    = _.chain(repo.jobs).map((j) => cron.next(j.cron, after)).min().value()
              ;

            //   winston.debug('after', new Date(after).toTimeString(), 'new at', new Date(at).toTimeString());
            return [ at, keys[index] ];
          })
          .then((tuples) => {
            var op    = _.groupBy(tuples, (t) => t[0]? 'add' : 'rem')
              , wait  = []
              ;

            if (_.size(op['add'])) {
              let arg = _.concat('cron', _.flatten(op['add']));
              console.log('add', arg);
              wait.push(redis.zaddAsync(arg));
            }

            if (_.size(op['rem'])) {
              let arg = _.concat('cron', _.flatten(_.map(op['rem'], (t) => t[1])));
              console.log('rem', arg);
              wait.push(redis.zremAsync(arg));
            }

            return Promise.all(wait);
          })
          .catch((err) => {
            winston.error(err);
            // throw err;
          })
          .asCallback(cb);
}, 10 * 1000);

// todo [akamel] this is a problem.. first it does 0 => 0 whe consumer is not up, second, we don't need it to run every 50ms... just detect change to min
async.forever(
  (next) => {
    var take = 10 * 1000;
    redis.zrangebyscore(['cron', 0, '(' + max, 'WITHSCORES', 'LIMIT', 0, take], (err, res) => {
      var chunks  = _.chunk(res, 2)
        , keys    = _.map(chunks, (c) => c[0])
        ;

      winston.debug(new Date().toTimeString(), 'zrangebyscore', 0, '(', new Date(_.parseInt(max)).toTimeString(), 'WITHSCORES', 'LIMIT', 0, take, 'found', _.size(chunks));
      if (_.size(chunks)) {
        cargo.push(keys, _.after(_.size(chunks), next));
      } else {
        _.delay(next, 10 * 1000);
      }
    });
  },
  (err) => {
      // if next is called with a value in its first parameter, it will appear
      // in here as 'err', and execution will stop.
  }
);

var app = express();

app.use(bodyParser.json());

app.post('/cron', (req, res, next) => {
  var remote          = req.body.remote   //|| 'https://github.com/a7medkamel/taskmill-core-agent.git'
    , branch          = req.body.branch   || 'master'
    , text            = req.body.text
    ;
  // res.sendStatus(202);
  Promise
    .try(() => {
      if (_.isEmpty(text)) {
        return [];
      }

      return cron
              .parse(text)
              // .catchThrow((err) => {
              .catch((err) => {
                winston.error('parse', remote, branch, text);
              });
    })
    .then((jobs) => {
      // todo [akamel] if jobs is empty, delete entry
      var key   = 'cron:repository:' + remote + ':' + branch
        , data  = {
            remote  : remote
          , branch  : branch
          , jobs    : jobs
        };

      return redis
              .setAsync(key, JSON.stringify(data))
              .then(() => {
                return Promise.fromCallback((cb) => cargo.push(key, cb))
              })
              .then(() => {
                res.send(data);
                winston.info('updated', remote, branch, text);
              });
    })
    ;
});

function boot() {
  // 1. delete the sorted set
  return redis
          .delAsync('cron')
          .then(() => {
            var waits = [];
            // 2. scan all cron jobs
            return Promise
                    .fromCallback((cb) => {
                      var cursor = 0;
                      async.doWhilst((callback) => {
                        redis.scan([cursor, 'MATCH', 'cron:repository:*', 'COUNT', /*10 **/ 1000], (err, res) => {
                          cursor = res[0];
                          // 3. reschedule those keys

                          if (_.size(res[1])) {
                            waits.push(Promise.fromCallback((cb) => cargo.push(res[1], _.after(_.size(res[1]), cb))));
                          }
                          callback(undefined, res[0]);
                        });
                      }, (cursor) => cursor != 0, cb);
                    })
                    // todo [akamel] do we need to wait? why not just let them start acting on the ones we already inserted?
                    .then(() => {
                      winston.info('done scan');
                      return Promise.all(waits);
                    });
          })
          .then(() => {
            return Promise.promisify(app.listen, { context : app})(config.getUrlObject('cron.scheduler').port);
          })
          .catch((err) => {
            winston.error('error booting scheduler', err);
          });
}

boot();
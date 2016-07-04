var Promise     = require('bluebird')
  , winston     = require('winston')
  , _           = require('lodash')
  , config      = require('config-url')
  , async       = require('async')
  , rp          = require('request-promise')
  , redis       = require('../redis').connect()
  , pub         = require('../redis').connect()
  ;

winston.level = config.get('cron.winston.level');

redis.on('error', (err) => {
  winston.error('redis', err);
});

pub.on('error', (err) => {
  winston.error('redis', err);
});

// todo [akamel] can we have a race condition where we update the db and end up with2 entries in the 'set'?
// var limit = config.get('cron.batch.read');
function consume(min, max) {
  return Promise
          .fromCallback((cb) => {
            var offset  = 0
              , take    = 10 * 1000
              ;

            async.doWhilst((callback) => {
              redis.zrangebyscore(['cron', min, '(' + max, 'WITHSCORES', 'LIMIT', offset, take], (err, res) => {
                var chunks = _.chunk(res, 2);

                offset += _.size(chunks);
                // 3. exec these repos
                if (_.size(chunks)) {
                  _.each(chunks, (chunk) => {
                    // todo [akamel] use url join
                    // todo [akamel] send one req instead of each chunk?
                    rp.post({ 
                        url   : config.getUrl('cron.exec') + '/exec'
                      , json  : true
                      , body  : {
                            key : chunk[0]
                          , at  : parseInt(chunk[1])
                        }
                    });
                  });

                  var f = _.parseInt(_.first(chunks)[1])
                    , t = _.parseInt(_.last(chunks)[1])
                    ;

                  winston.debug(new Date().toTimeString(), offset, 'from', new Date(f).toTimeString(), 'to', new Date(t).toTimeString());
                }

                callback(undefined, _.size(chunks));
              });
            }, (c) => c === take, cb);
          });
}

function next_range() {
  // todo [akamel] this might result in us working on a range that has passed, if we spent more than 60+ sec running last range
  min = max;
  max += 60 * 1000; // move to next min

  pub.publish('cron:cursor', min);
}

var now = new Date();

now.setSeconds(0);
now.setMilliseconds(0);

// snap min and max to minute boundry
var min     = now.getTime()
  , max     = min + 60 * 1000
  ;

pub.publish('cron:cursor', min);

async.forever(
  (next) => {
    consume(min, max)
      .then(() => {
        return next_range();
      })
      .asCallback((err, res) => {
        var sleep_for = Math.max(0, (min - (new Date().getTime())));
        winston.debug('sleep_for', sleep_for);
        _.delay(next, sleep_for);
      });
  },
  (err) => {
      // if next is called with a value in its first parameter, it will appear
      // in here as 'err', and execution will stop.
  }
);

// todo consider 2 sets instead of one
// one for even, one for odd minutes
// that way we don't need to signal, while consumer is on even, producer is on odd and they swap
// that way they can work at the exact same time...
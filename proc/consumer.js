var Promise       = require('bluebird')
  , winston       = require('winston')
  , _             = require('lodash')
  , cron          = require('../lib/cron')
  , set           = require('../lib/data/set')
  , db            = require('../lib/data/db')
  , invalidation  = require('../lib/data/invalidation')
  ;

winston.level = 'info';

setInterval(consume, 250);

// todo [akamel] can we have a race condition where we update the db and end up with2 entries in the 'set'?
function consume() {
  set
    .pull()
    .then((result) => {
      // no need to wait for this
      invalidation.madd(result.data)
        .catch((err) => {
          winston.error('scheduler', 'invalidate', 'background', err);
        });

      return db
              .mget(result.data)
              .map((data, idx) => {
                // 1. get jobs to run
                var after   = result.data[idx].at
                  , with_at = _.chain(data.jobs)
                                .map((j) => _.extend({ at : cron.next(j.cron, after) }, j))
                                .value()
                  , current = _.filter(with_at, (j) => j.at === after)
                  , count   = _.size(current)
                  ;

                // 2. send command to relay
                // var now = new Date();
                // winston.info('scheduler', 'run', now.toTimeString(), now.getTime(), current);
                if (count === 0) {
                  winston.warn('scheduler', 'run', 'no items found to run');
                }

                return count; 
              })
              // todo [akamel] there might be a race condition, saw 4 sets of 10,000 run at 2:00 instead of 3 sets
              .tap((result) => {
                var count = _.sum(result);

                if (count) {
                  winston.info('scheduler', 'run', 'count', new Date().toTimeString(), count);
                }
              });
    })
    .catch((err) => {
      winston.error('scheduler', 'pull', 'background', err);
    });
}
var Promise       = require('bluebird')
  , winston       = require('winston')
  , _             = require('lodash')
  , cron          = require('../lib/cron')
  , set           = require('../lib/data/set')
  , db            = require('../lib/data/db')
  , invalidation  = require('../lib/data/invalidation')
  ;

winston.level = 'info';

setInterval(produce, 500);

function produce() {
  var now = new Date().getTime();

  invalidation
    .pull()
    .then((result) => {
      return db
              .mget(result.data)
              .map((data) => {
                var after = now + 1000
                  , at    = _.chain(data.jobs).map((j) => cron.next(j.cron, after)).max().value()
                  ;

                winston.debug('after', new Date(after).toTimeString(), 'new at', new Date(at).toTimeString());

                // 1. get next @at time to run this repo
                return _.defaults({ at : at }, data);
              })
              .then((arr) => {
                // 2. send command to relay
                return set.madd(arr);
              });
    })
    .catch((err) => {
      winston.error('scheduler', 'reschedule', 'background', err);
    });
}
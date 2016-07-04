var Promise     = require('bluebird')
  , config      = require('config')
  , _           = require('lodash')
  , winston     = require('winston')
  , redis       = require('../lib/redis').connect()
  ;

winston.level = 'info';

redis.on('error', (err) => {
  winston.error('redis', err);
});

var crons = [
    '* * * * *'
  , '*/2 * * * *'
  , '*/5 * * * *'
];

function seed(remote, branch, cron) {
  var remote  = remote
    , branch  = branch
    , cron    = cron
    ;

  var data = {
      remote  : remote
    , branch  : branch
    , jobs    : [{
        cron      : cron,
        command   : 'http://' + remote + '#' + branch,
        comment   : 'no comment'
    }]
  };

  return data;
}

function seed_many(n) {
  return Promise
          .map(crons, (cron, cron_id) => {
            return Promise
                    .map(_.times(n/1000), (z, chunk_id) => {
                      var repos = _.times(1000, (id) => {
                        return seed('mock', cron_id + '-' + chunk_id + '-' + id, cron);
                      });

                      if (_.size(repos)) {
                        var args = _.chain(repos).map((repo) => [ 'cron:repository:' + repo.remote + ':' + repo.branch, JSON.stringify(repo) ]).flatten().value();
                        return redis.msetAsync(args);
                      }
                    }, { concurrency : 10 });
          });
}

redis
  .flushdbAsync()
  .then(() => {
      // seed('* * * * *', 'master');
      // seed_many(5, 0);
      // return seed_many(2 * 1000);
      return seed_many(30 * 1000);
      // seed_many(100 * 1000, 100 * 1000);
      // seed_many(100 * 1000, 200 * 1000);
  })
  .then(() => {
    winston.info('seeded');
    process.exit();
  });

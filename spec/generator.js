var Promise     = require('bluebird')
  , config      = require('config')
  , _           = require('lodash')
  , winston     = require('winston')
  , db          = require('../lib/data/db')
  ;

winston.level = 'info';

var crons = [
    '* * * * *'
  , '*/2 * * * *'
  , '*/5 * * * *'
];

db.flush();

// seed('* * * * *', 'master');
seed_many(100 * 1000, 0);
seed_many(100 * 1000, 100 * 1000);
seed_many(100 * 1000, 200 * 1000);
// seed_many(5);

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

function seed_many(n, offset) {
  var arr = [];
  _.each(crons, (cron, index) => {
    _.times(n, (b) => {
      var ret = seed('mock', index + '-' + (b + offset), cron);
      arr.push(ret);
    });
  });

  db.mset(arr);
}
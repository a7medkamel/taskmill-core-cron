var Promise       = require('bluebird')
  , winston       = require('winston')
  , config        = require('config')
  , _             = require('lodash')
  , redis         = require('redis')
  , invalidation  = require('./invalidation')
  ;

var client = redis.createClient();

client.on('error', function (err) {
  winston.error('redis', err);
});

// function set(remote, branch, data, cb) {
//   winston.debug('db', 'set', remote, branch, data);

//   return Promise
//           .fromCallback((cb) => {
//             client.set('cron:repository:' + remote + ':' + branch, JSON.stringify(data), cb);
//           })
//           .then(() => {
//             return invalidation.add([{ remote : remote, branch : branch }]);
//           })
//           .asCallback(cb);
// }

// function get(remote, branch, cb) {
//   return Promise
//           .fromCallback((cb) => {
//             client.get('cron:repository:' + remote + ':' + branch, cb);
//           })
//           .then((json) => {
//             winston.debug('db', 'get', remote, branch, json);
//             return JSON.parse(json);
//           })
//           .asCallback(cb);
// }

function mget(repos, cb) {
  if (_.size(repos) === 0) {
    winston.debug('db', 'mget', '[skipped]');
    return Promise.resolve([]).asCallback(cb);
  }

  return Promise
          .fromCallback((cb) => {
            var args = _.map(repos, (repo) => 'cron:repository:' + repo.remote + ':' + repo.branch);
            client.mget(args, cb);
          })
          .then((arr) => {
            winston.debug('db', 'mget', repos, arr);
            return _.map(arr, (json) => JSON.parse(json));
          })
          .asCallback(cb);
}

function mset(repos, cb) {
  if (_.size(repos) === 0) {
    winston.debug('db', 'mset', '[skipped]');
    return Promise.resolve([]).asCallback(cb);
  }

  return Promise
          .fromCallback((cb) => {
            var args = _.chain(repos).map((repo) => [ 'cron:repository:' + repo.remote + ':' + repo.branch, JSON.stringify(repo) ]).flatten().value();
            console.log(args);
            client.mset(args, cb);
          })
          .then(() => {
            return invalidation.madd(repos);
          })
          .asCallback(cb);
}

function flush(cb) {
  winston.debug('db', 'flush');
  
  return Promise
          .fromCallback((cb) => {
            client.flushdb(cb);
          })
          .asCallback(cb);
}

module.exports = {
    // set     : set
  // , get     : get
    mset    : mset
  , mget    : mget
  , flush   : flush
};

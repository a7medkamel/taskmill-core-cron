var Promise     = require('bluebird')
  , winston     = require('winston')
  , _           = require('lodash')
  , config      = require('config-url')
  , express     = require('express')
  , bodyParser  = require('body-parser')
  , rp          = require('request-promise')
  , redis       = require('../redis').connect()
  , cron        = require('../cron')
  , urljoin     = require('url-join')
  , parsecurl   = require('parse-curl')
  ;

winston.level = config.get('cron.winston.level');

redis.on('error', (err) => {
  winston.error('redis', err);
});

var app = express();

app.use(bodyParser.json());

app.post('/exec', (req, res, next) => {
  var key = req.body.key
    , at  = req.body.at
    ;

  res.sendStatus(202);
  
  redis
    .getAsync(key)
    .then(JSON.parse)
    .then((repo) => {
      // 1. get jobs to run
      var next = _.chain(repo.jobs).map((j) => _.extend({ at : cron.next(j.cron, at) }, j))
        , due  = next.filter((j) => j.at === at)
        ;

      return Promise
              .resolve(due.value())
              .tap((due) => {
                if (_.size(due) === 0) {
                  winston.warn('exec', 'no items found to run', key, at, next.value());
                }
              })
    })
    .map((due) => {
      // 2. execute command
      return Promise
              .resolve(parsecurl(due.command))
              .then((curl) => {
                if (!curl) {
                  throw new Error('parse-curl "' + key + '" "' + due.command + '"');
                }
                return curl;
              })
              .then((curl) => {
                return rp({ 
                            url     : curl.url
                          , headers : curl.headers
                          , method  : curl.method
                          , body    : curl.body
                        })
                        .then((res) => { console.log(res); });
              })
              .catch((err) => {
                winston.warn('exec', err);
              });
    });
});

function listen(options, cb) {
  return Promise
          .promisify(app.listen, { context : app})(options.port)
          .nodeify(cb);
}

listen({
  port : config.getUrlObject('cron.exec').port
});
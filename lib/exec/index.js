var Promise     = require('bluebird')
  , winston     = require('winston')
  , _           = require('lodash')
  , config      = require('config-url')
  , express     = require('express')
  , bodyParser  = require('body-parser')
  , request     = require('request')
  , redis       = require('../redis').connect()
  , cron        = require('../cron')
  , url         = require('url')
  , path        = require('path')
  , parsecurl   = require('parse-curl')
  , onFinished  = require('on-finished')
  , account_sdk = require('taskmill-core-account-sdk')
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
      let { remote, branch, jobs } = repo;

      // ex: https://github.com/codingdawg/taskmill-help.git
      let parsed    = url.parse(remote)
        , hostname  = parsed.hostname
        // ex: /a7medkamel/taskmill-help.git
        , username  = parsed.pathname.split(path.sep)[1]
        ;

      // todo [akamel] get token to run private cron
      return account_sdk
              .issueTokenByUsername(hostname, username)
              .catch({ message : 'not found' }, () => {})
              .then((jwt) => {
                // 1. get jobs to run
                let next = _.chain(jobs).map((j) => _.extend({ at : cron.next(j.cron, at) }, j))
                  , due  = next.filter((j) => j.at === at)
                  ;

                return Promise
                        .resolve(due.value())
                        .tap((due) => {
                          if (_.size(due) === 0) {
                            winston.warn('exec', 'no items found to run', key, at, next.value());
                          }
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
                                    let headers = curl.headers;
                                    if (jwt) {
                                      headers = _.extend({ authorization : 'Bearer ' + jwt }, headers);
                                    }

                                    let output = request({
                                                      url     : curl.url
                                                    , headers : headers
                                                    , method  : curl.method
                                                    , body    : curl.body
                                                  });

                                    let pipe = _.get(headers, 'pipe');
                                    if (pipe) {
                                      output.pipe(request({ method : 'POST', url : pipe }));
                                    }

                                    return Promise
                                            .fromCallback((cb) => {
                                              onFinished(output, cb);
                                            });
                                    // .then((res) => { winston.debug(res); });
                                  })
                                  .catch((err) => {
                                    winston.warn('exec', due, err.toString());
                                  });
                        });
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

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
                                    let { header } = curl;
                                    if (jwt) {
                                      header = _.extend({ authorization : 'Bearer ' + jwt }, header);
                                    }

                                    let stream = request({
                                                      url     : curl.url
                                                    , headers : header
                                                    , method  : curl.method
                                                    , body    : curl.body
                                                  });

                                    // todo [akamel] this will double pipe if the url is going to the gateway
                                    let pipe = _.get(header, 'pipe');
                                    if (pipe) {
                                      stream.pipe(request({ method : 'POST', url : pipe }));
                                    }

                                    // .then((res) => { winston.debug(res); });
                                    stream.pipe(process.stdout);

                                    return Promise.fromCallback((cb) => { onFinished(stream, cb); });
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

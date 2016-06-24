var express     = require('express')
  , Promise     = require('bluebird')
  , bodyParser  = require('body-parser')
  , winston     = require('winston')
  , config      = require('config')
  , _           = require('lodash')
  , set         = require('./data/set')
  , db          = require('./data/db')
  , cron        = require('./cron')
  ;

// winston.level = 'debug';
winston.level = 'info';

var app = express();

app.use(bodyParser.json());

app.post('/cron', (req, res, next) => {
  var remote          = req.body.remote   //|| 'https://github.com/a7medkamel/taskmill-core-agent.git'
    , branch          = req.body.branch   || 'master'
    , text            = req.body.text
    ;

  res.sendStatus(202);
  cron
    .parse(text)
    .then((result) => {
      db.set(remote, branch, result.data);

      set.add(remote, branch, result.at);
    })
    ;
});

app.get('/pending', (req, res, next) => {
  set
    .pending()
    .then((result) => {
      res.send(result);
    });
});

app.get('/peek', (req, res, next) => {
  set
    .peek()
    .then((result) => {
      res.send(result);
    });
});

app.get('/pull', (req, res, next) => {
  set
    .pull()
    .then((result) => {
      res.send(result);
    });
});

// app.get('/trim', (req, res, next) => {
//   set
//     .trim(1000 * 1000)
//     .then((result) => {
//       res.send(result);
//     });
// });

function listen(options, cb) {
  return Promise
          .promisify(app.listen, { context : app})(options.port)
          .nodeify(cb);
}

module.exports = {
    listen : listen
};
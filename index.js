var config    = require('config-url')
  , winston   = require('winston')
  , Promise   = require('bluebird')
  , http      = require('./lib')
  , scheduler = require('./lib/scheduler')
  ;

process.on('uncaughtException', function (err) {
  console.error(err.stack || err.toString());
});

function main() {
  return Promise
          .all([
              http
                .listen({ port : config.getUrlObject('codedb').port })
                .then(() => {
                  winston.info('taskmill-core-cron [started] :%d', config.getUrlObject('codedb').port);
                })
            , scheduler.monitor()
          ]);
}

if (require.main === module) {
  main();
}

module.exports = {
  main  : main
};
var Promise     = require('bluebird')
  , winston     = require('winston')
  , _           = require('lodash')
  , CronTabJob  = require('crontab/lib/CronJob')
  // , cron_parser = require('cron-parser')
  , later       = require('later')
  ;

function parse(text, cb) {
  return Promise
          .try(() => {
            return text.split('\n');
          })
          .map((line) => {
            return Promise
                    .try(() => new CronTabJob(line))
                    .then((job) => {
                      if (_.isNull(job) || _.isUndefined(job) || !job.isValid()) {
                        return { error : { message : 'invalid job' } };
                      }

                      return {
                          cron      : _.map([ job.minute(), job.hour(), job.dom(), job.month(), job.dow() ], (t) => t.toString()).join(' ')
                        , command   : job.command()
                      };
                    });
          })
          .then((jobs) => {
            var arr = _.chain(jobs)
                        .map((job) => {
                          if (!job.error) {
                            return {
                              cron      : job.cron,
                              command   : job.command,
                              comment   : job.comment,
                              at        : next(job.cron)
                              // 'in'      : (next.getTime() - new Date().getTime())
                              // timeZone  : 'UTC'
                            };
                          }
                        })
                        .compact()
                        .value();

            return {
                data : {
                    remote  : remote
                  , branch  : branch
                  , jobs    : _.map(arr, (job) => _.omit(job, 'at') )
                }
              , at   : _.minBy(arr, 'at').at
            };
          })
          .toCallback(cb);
}

var cache = {};

function next(cron, after) {
  var exp = cache[cron];
  if (!exp) {
    exp = cache[cron] = later.parse.cron(cron);
  }

  // return later.schedule(exp).next(1, new Date(after));
  return later.schedule(exp).next(1, after).getTime();
  // var obj = cron_parser.parseExpression(cron, { 
  //                           tz          : 'UTC'
  //                         , currentDate : after
  //                       }).next();

  // var ret = obj.getTime();
  // return ret;
}

module.exports = {
    parse : parse
  , next  : next
};
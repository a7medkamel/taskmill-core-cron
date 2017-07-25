var Promise     = require('bluebird')
  , winston     = require('winston')
  , _           = require('lodash')
  , CronTabJob  = require('crontab/lib/CronJob')
  , later       = require('later')
  , config      = require('config')
  ;

const limit = config.get('cron.limit_per_crontab');

function parse(text, cb) {
  return Promise
          .try(() => {
            return _
                    .chain(text.split('\n'))
                    .reject(_.isEmpty)
                    .take(limit)
                    .value()
                    ;
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
            var ret = _.chain(jobs)
                        .map((job) => {
                          if (!job.error) {
                            return {
                              cron      : job.cron,
                              command   : job.command,
                              comment   : job.comment,
                              // at        : next(job.cron)
                              // 'in'      : (next.getTime() - new Date().getTime())
                              // timeZone  : 'UTC'
                            };
                          }
                        })
                        .compact()
                        .value();

            // return {
            //     data : {
            //         remote  : remote
            //       , branch  : branch
            //       , jobs    : _.map(arr, (job) => _.omit(job, 'at') )
            //     }
            //   , at   : _.minBy(arr, 'at').at
            // };
            return ret;
          })
          .asCallback(cb);
}

var cache = {};

function next(cron, after) {
  var exp = cache[cron];
  if (!exp) {
    exp = cache[cron] = later.parse.cron(cron);
  }

  var at = later.schedule(exp).next(1, after).getTime();

  // trim ms because if we round trip it, we might end up with error from later
  // return Math.round(at/6000) * 6000;
  return at;
}

module.exports = {
    parse : parse
  , next  : next
};

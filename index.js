/**
 *
 * Transaction of sequential steps, with retry and rollback.
 * Example of step added to a transaction:
 *
 * {
 *      name: <string> || null
 *      action:   function(state, callback(err, [update])), // err can have optional field retry = true
 *      rollback: function(state, callback(err, [update]))
 * };
 *
 * Rollback can also be retried if it fails with err.retry = true.
 *
 */

var _ = require('lodash');

const defaults = {
    steps: [],
    maxRetryDelay: 5000,
    retryDelay:    1000,
    retryDelayMultiplier:   1.3,
    onError: function(err) { return err; }
};

module.exports = function(options) {

    options = _.merge({}, defaults, options || {});
    let steps = options.steps;

    this.add = function(step) {

        if (!step.action) {
            step.action = function(state, callback) {
                return callback(null, state);
            };
        }

        if (!step.rollback) {
            step.rollback = function(state, callback) {
                return callback(null, state);
            };
        }

        steps.push(step);
    };

    this.execute = function(state, callback) {

        if (steps.length == 0) {
            return callback(null, state);
        }

        state = _.clone(state);

        let retryDelay = options.retryDelay;
        let i = 0;
        let rollback = false;
        let error = null;

        let next = function(state) {
            let step = steps[i];

            if (!step) {
                return setImmediate(callback, error, state);
            }

            let name = step.name || 'step'; // should not be empty
            let fn = rollback ? step.rollback : step.action;

            try {

                return fn(state, function(err, update) {
                    return cb(err, _.merge({}, state, update || {}));
                });

            } catch (exception) {
                return cb(exception, state);
            }
        };

        var cb = function(err, state) {

            if (err) {
                err = options.onError(err);
            }

            if (err && err.retry) {
                setTimeout(next, retryDelay, state);
                retryDelay = Math.min(options.maxRetryDelay, Math.floor(retryDelay * options.retryDelayMultiplier));
                return;
            }

            if (err && rollback) {
                return callback(new Error('Flow rollback error: ' + err.message + ', during rollback caused by error: ' + error.message), state);
            }

            if (err) {
                rollback = true;
                error = err;
            }

            i += rollback ? -1 : +1;

            return setImmediate(next, state);
        };

        setImmediate(next, state);

    };

};

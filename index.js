/**
 * Module dependencies
 */
var common = require('./lib/common');
var connect = require('mongodb-async').connect;
var Queue = require('./lib/queue');
var Worker = require('./lib/worker');

/**
 * Queue status constants
 */

exports.STATE_ERROR = common.STATE_ERROR;
exports.STATE_NEW = common.STATE_NEW;
exports.STATE_SHIFTED = common.STATE_SHIFTED;
exports.STATE_INPROGRESS = common.STATE_INPROGRESS;
exports.STATE_PARTIAL = common.STATE_PARTIAL;
exports.STATE_FINISHED = common.STATE_FINISHED;

/**
 * Connect to mongodb, create and return an instance of `Queue`
 *
 * @param options
 * @api public
 */
exports.createQueue = function(options) {
  return new Queue(getDbObj(options));
};

/**
 * Connect to mongodb, create and return an instance of `Worker`
 *
 * @param options
 * @api public
 */
exports.createWorker = function(options) {
  return new Worker(getDbObj(options));
};

/**
 * Create a mongodb-async instance according to options
 * @param options
 * @return {Object}
 */
function getDbObj(options) {
  options = common.setDefaultOptions(options);
  var server = connect(options.host, options.port, {poolSize: options.poolSize});
  var db = server.db(options.db);
  return db;
}
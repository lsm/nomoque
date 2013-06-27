/**
 * Module dependencies
 */
var nmq = process.env.NMQ_COV ? require('./lib-cov') : require('./lib');
var common = nmq.common;
var Queue = nmq.Queue;
var MongodbAsync = require('mongodb-async');
var connect = MongodbAsync.connect;

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
exports.createQueue = function (options) {
  var db;
  if (options.db) {
    db = options;
  } else {
    options = common.setDefaultOptions(options);
    var server = connect(options.dbHost, options.dbPort, {poolSize: options.dbPoolSize});
    db = server.db(options.dbName);
  }
  return new Queue(db, options);
};

/**
 * Exports Queue
 */
exports.Queue = Queue;
/**
 * Module dependencies
 */
var common = require('./lib/common');
var MongodbAsync = require('mongodb-async');
var connect = MongodbAsync.connect;
var AsyncDb = MongodbAsync.AsyncDb;
var Queue = require('./lib/queue');

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
    var server = connect(options.host, options.port, {poolSize: options.poolSize});
    db = server.db(options.name);
  }
  return new Queue(db);
};

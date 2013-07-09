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
  options = setDefaultOptions(options);
  var server = connect(options.dbHost, options.dbPort, {poolSize: options.dbPoolSize});
  var db = server.db(options.dbName);
  ensureIndexes(db);
  return new Queue(db, options);
};

/**
 * Private helper functions
 */

function setDefaultOptions(options) {
  options = options || {};
  options.dbHost = options.dbHost || '127.0.0.1';
  options.dbPort = options.dbPort || 27017;
  options.dbPoolSize = options.dbPoolSize || 2;
  options.dbName = options.dbName || 'nomoque_default_queue';
  return options;
}

function ensureIndexes(db) {
  db.collection('queues').ensureIndex([
    ['date', 1],
    ['topic', 1],
    ['state', 1]
  ]);
  // index for `results`
  db.collection('results').ensureIndex([
    ['date', 1],
    ['topic', 1],
    ['state', 1],
    ['created', -1],
    ['shifted', -1],
    ['finished', -1]
  ]);
}
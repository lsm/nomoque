/**
 * Module dependencies
 */
var nmq = process.env.NMQ_COV ? require('./lib-cov') : require('./lib');
var Queue = nmq.Queue;
var MongodbAsync = require('mongodb-async');
var connect = MongodbAsync.connect;


/**
 * Exports Queue
 */

exports.Queue = Queue;

/**
 * Queue status constants
 */

exports.STATE_ERROR = Queue.QueueState.STATE_ERROR;
exports.STATE_NEW = Queue.QueueState.STATE_NEW;
exports.STATE_SHIFTED = Queue.QueueState.STATE_SHIFTED;
exports.STATE_INPROGRESS = Queue.QueueState.STATE_INPROGRESS;
exports.STATE_PARTIAL = Queue.QueueState.STATE_PARTIAL;
exports.STATE_FINISHED = Queue.QueueState.STATE_FINISHED;

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
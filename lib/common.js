/**
 * Queue status constants
 */

exports.STATE_ERROR = -1;
exports.STATE_NEW = 0;
exports.STATE_SHIFTED = 1;
exports.STATE_PARTIAL = 2;
exports.STATE_FINISHED = 3;


/**
 * Private fucntions
 */

exports.setDefaultOptions = function(options) {
  options = options || {};
  options.dbHost = options.dbHost || '127.0.0.1';
  options.dbPort = options.dbPort || 27017;
  options.dbName = options.dbName || 'nomoque_default_queue';
  return options;
};

exports.ensureIndexes = function(db) {
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
};
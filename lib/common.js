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
  options.host = options.host || '127.0.0.1';
  options.port = options.port || 27017;
  options.db = options.db || 'nmq';
  return options;
}

exports.getDateString = function(date) {
  date = date || new Date;
  var year = date.getUTCFullYear();
  var month = date.getUTCMonth() + 1;
  var day = date.getUTCDate();
  if (month < 10) month = '0' + month;
  if (day < 10) day = '0' + day;
  return '' + year + month + day;
}

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
    ['name', 1],
    ['state', 1],
    ['created', -1],
    ['shifted', -1],
    ['finished', -1]
  ]);
  // index for `errors`
  db.collection('errors').ensureIndex([
    ['date', 1],
    ['topic', 1],
    ['name', 1],
    ['mid', 1],
    ['started', -1],
    ['finished', -1]
  ]);
};
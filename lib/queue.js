/**
 * Module dependencies
 */
var EventEmitter = require('events').EventEmitter;
var common = require('./common');


/**
 * Queue
 */
var Queue = module.exports = function Queue(db) {
  common.ensureIndexes(db);
  this.db = db;
  this.queColl = db.collection('queues');
  this.configColl = db.collection('configs')
};

/**
 * Inherits `EventEmitter`
 */
Queue.prototype.__proto__ = EventEmitter.prototype;

/**
 * Push a message to a topic queue
 *
 * @param {String} topic
 * @param {Object} message
 */
Queue.prototype.push = function(topic, message) {
  var self = this;

  function saveMsg(defer, nextIdDoc) {
    var qid = nextIdDoc.currentId;
    var date = new Date;
    var msg = {_id: qid, topic: topic, message: message,
      state: common.STATE_NEW,
      date: common.getDateString(date),
      created: date
    };
    self.queColl.insert(msg, {safe: true}).then(
      function(doc) {
        if (doc) {
          self.emit('queued', Array.isArray(doc) ? doc[0] : doc);
        }
      }).fail(defer.error);
  }

  nextQueueId(this.configColl).and(saveMsg).fail(function(err) {
    self.emit('fault', err);
  });
};

Queue.prototype.close = function() {
  this.db.close();
};

function nextQueueId(coll) {
  return coll.findAndModify({_id: 'nextQueueId'}, [], {$inc: {currentId: 1}}, {'new': true, upsert: 'true'});
}
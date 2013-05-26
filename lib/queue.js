/**
 * Module dependencies
 */
var EventEmitter = require('events').EventEmitter;
var common = require('./common');


/**
 * Queue
 */
var Queue = module.exports = function Queue(db) {
  EventEmitter.call(this);
  common.ensureIndexes(db);
  this._tasks = {};
  this._topics = [];
  this.delay = 1;
  this._state = 'stopped';
  this._maxWorkers = 2;
  this._currentWorkers = 0;
  this.db = db;
  this.queColl = db.collection('queues');
  this.resultColl = db.collection('results');
  this.errColl = db.collection('errors');
  this.configColl = db.collection('configs');
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
Queue.prototype.push = function (topic, message) {
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
      function (doc) {
        if (doc) {
          self.emit('queued', Array.isArray(doc) ? doc[0] : doc);
        }
      }).fail(defer.error);
  }

  nextQueueId(this.configColl).and(saveMsg).fail(function (err) {
    self.emit('fault', err);
  });
};


/**
 * Register a named task for specific topic
 *
 * @param {String} topic Topic of the queue
 * @param {String} name Name of the task
 * @param {Function} fn
 */
Queue.prototype.process = function (topic, name, fn) {
  var taskHash = {};
  if (typeof name === 'object') {
    taskHash = name;
  } else {
    taskHash[name] = fn;
  }
  if (this._topics.indexOf(topic) === -1) {
    this._topics.push(topic);
  }
  if (!this._tasks[topic]) {
    this._tasks[topic] = [];
  }
  var self = this;
  Object.keys(taskHash).forEach(function (taskName) {
    self._tasks[topic].push({name: taskName, fn: taskHash[taskName]});
  });
};

/**
 * Move message from one collection to another
 *
 * @param message
 * @param fromColl
 * @param toColl
 * @returns {*}
 * @private
 */
Queue.prototype._mvMessages = function (message, fromColl, toColl) {
  // update don't like `_id`
  var id = message._id;
  delete message['_id'];
  // upsert the message into `toColl`
  return toColl.update({_id: id}, {$set: message}, {safe: true, upsert: true})
    .and(function (defer) {
      // remove the old one from `from`
      fromColl.remove({_id: id}, {safe: true})
        .then(function () {
          message._id = id;
          defer.next(message);
        }).fail(defer.error);
    }).fail(this._onFault(this, true));
};

Queue.prototype._shift = function () {
  if (this._state === 'stopped' || this._currentWorkers++ >= this._maxWorkers) {
    return;
  }
  var self = this;
  // get messages we are interested
  this.queColl.findAndModify({topic: {$in: self._topics}, state: common.STATE_NEW},
    // use `nature` order
    [],
    // modify the message status
    {$set: {shifted: new Date, state: common.STATE_SHIFTED}},
    // return updated message
    {remove: false, 'new': true, upsert: false}).then(function (message) {
      if (!message) {
        tryNext();
        return;
      }
      // handle shifted message
      // move the message from `queues` to collection `results`
      self._mvMessages(message, self.queColl, self.resultColl).then(function (message) {
        // perform tasks
        self._process(message);
      });
    }).fail(tryNext);
  function tryNext() {
    // if nothing, wait for `delay` and try shift again
    self._currentWorkers--;
    setTimeout(function () {
      self._shift();
    }, self.delay * 1000);
  }
};

Queue.prototype._process = function (message) {
  var tasks = this._tasks[message.topic];
  var self = this;
  var count = 0;
  var partial = false;
  var hasResult = false;
  var results = {};

  function checkFinish() {
    if (++count === tasks.length) {
      // update state and results
      var updateDoc = {
        finished: new Date,
        state: partial ? common.STATE_PARTIAL : common.STATE_FINISHED,
        results: hasResult ? results : undefined
      };
      self.resultColl
        .update({_id: message._id}, {$set: updateDoc}, {safe: true, upsert: false})
        .then(function () {
          self.emit('finished', message.topic, results);
          // shift next
          self._currentWorkers--;
          self._shift();
        }).fail(self._onFault(self, true));
    }
  }

  // run each tasks
  tasks.forEach(function (task) {
    var log = {
      topic: message.topic,
      mid: message._id,
      name: task.name,
      started: new Date(),
      state: common.STATE_INPROGRESS
    };
    try {
      task.fn.call(message, message.message, function (taskErr, result) {
        var ts = new Date();
        log.finished = ts;
        log.date = common.getDateString(ts);
        if (taskErr) {
          partial = true;
          // record error
          log.state = common.STATE_ERROR;
          log.error = taskErr;
          self.errColl.insert(log, {safe: true}).fail(self._onFault(self));
          if (self.listeners('error').length > 0) {
            self.emit('error', log.error, log);
          }
        } else {
          log.state = common.STATE_FINISHED;
          // record result if any
          if (result !== undefined) {
            hasResult = true;
            results[task.name] = result;
          }
        }
        // update the log
        var msgLog = {
          name: log.name,
          started: log.started,
          finished: log.finished,
          state: log.state
        };
        self.resultColl
          .update({_id: message._id}, {'$push': {logs: msgLog}}, {safe: true, upsert: false})
          .then(checkFinish).fail(self._onFault(self));
      });
    } catch (e) {
      log.state = common.STATE_ERROR;
      log.error = e.stack || e;
      self.errColl.insert(log, {safe: true}).fail(self._onFault(self));
      if (self.listeners('error').length > 0) {
        self.emit('error', log.error, log);
      }
    }
  });
};

Queue.prototype._onFault = function (self, workerFinished) {
  return function onFault(err) {
    if (err) {
      self.emit('fault', err.stack || err);
    }
    if (workerFinished) {
      self._currentWorkers--;
    }
  };
};

Queue.prototype.start = function (workers) {
  this._state = 'running';
  if (workers) {
    this._maxWorkers = workers;
  }
  for (var i = 0; i < this._maxWorkers; i++) {
    this._shift();
  }
};

Queue.prototype.stop = function () {
  this._state = 'stopped';
};

Queue.prototype.close = function () {
  this.stop();
  this.db.close();
};

function nextQueueId(coll) {
  return coll.findAndModify({_id: 'nextQueueId'}, [], {$inc: {currentId: 1}}, {'new': true, upsert: 'true'});
}
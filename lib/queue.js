/**
 * Module dependencies
 */
var EventEmitter = require('events').EventEmitter;
var common = require('./common');


/**
 * Queue
 */
var Queue = module.exports = function Queue(db, options) {
  EventEmitter.call(this);
  common.ensureIndexes(db);
  this._tasks = {};
  this._topics = [];
  this.delay = 1;
  this._state = 'stopped';
  this._maxWorkers = 2;
  this._currentWorkers = 0;
  this.db = db;
  var queColl = 'queues';
  var resultColl = 'results';
  var configColl = 'configs';
  options = options || {};
  var collectionPrefix = options.collectionPrefix;
  if (collectionPrefix) {
    queColl = collectionPrefix + '_' + queColl;
    resultColl = collectionPrefix + '_' + resultColl;
    configColl = collectionPrefix + '_' + configColl;
  }
  this.queColl = db.collection(queColl);
  this.resultColl = db.collection(resultColl);
  this.configColl = db.collection(configColl);
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
    self.queColl.insert(msg, {safe: true}).then(function (doc) {
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

Queue.prototype._runTask = function (task, message, errors, results, done) {
  var self = this;
  var log = {
    _id: message._id,
    topic: message.topic,
    name: task.name,
    started: new Date(),
    state: common.STATE_INPROGRESS
  };
  try {
    task.fn.call(message, message.message, function (taskErr, result) {
      log.finished = new Date();
      if (taskErr) {
        // record error
        log.state = common.STATE_ERROR;
        log.error = taskErr;
        errors[task.name] = taskErr;
        self._log(log, done);
      } else {
        log.state = common.STATE_FINISHED;
        // record result if any
        if ('undefined' !== typeof result) {
          results[task.name] = result;
          log.result = result;
        }
        self._log(log, done);
      }
    });
  } catch (e) {
    log.state = common.STATE_ERROR;
    log.error = e.stack || e;
    log.finished = new Date();
    self._log(log, done);
  }
};

Queue.prototype._process = function (message) {
  var tasks = this._tasks[message.topic];
  var self = this;
  var count = 0;
  var results = {};
  var errors = {};

  function checkFinish() {
    if (++count === tasks.length) {
      if (Object.keys(errors).length > 0) {
        // errors found
        self.resultColl
          .update({_id: message._id}, {$set: {state: common.STATE_PARTIAL}}, {safe: true})
          .then(function () {
            self._emitOrLog('finished', [message.topic, errors, results]);
            // shift next
            self._currentWorkers--;
            self._shift();
          });
      } else {
        self._emitOrLog('finished', [message.topic, errors, results]);
        // shift next
        self._currentWorkers--;
        self._shift();
      }
    }
  }

  // run each tasks
  tasks.forEach(function (task) {
    self._runTask(task, message, errors, results, checkFinish);
  });
};

Queue.prototype._log = function (log, callback) {
  var logName = 'logs.' + log.name;
  var setDoc = {
    topic: log.topic,
    state: log.state
  };
  var pushDoc = {};
  pushDoc[logName] = {
    started: log.started,
    finished: log.finished,
    state: log.state
  };
  if (log.error) {
    pushDoc[logName].error = log.error;
  }
  if (log.result) {
    pushDoc[logName].result = log.result;
  }
  var deferred = this.resultColl
    .update({_id: log._id}, {$set:setDoc, $push: pushDoc}, {upsert: true, safe: true})
    .fail(this._onFault(this));
  if ('function' === typeof callback) {
    deferred.then(callback);
  }
  this._emitOrLog('error', [log]);
};

Queue.prototype._onFault = function (self, workerFinished) {
  return function onFault(err) {
    if (err) {
      self._emitOrLog('fault', [err.stack || err]);
    }
    if (workerFinished) {
      self._currentWorkers--;
    }
  };
};

Queue.prototype._emitOrLog = function (event, args) {
  if (this.listeners(event).length > 0) {
    args.unshift(event);
    this.emit.apply(this, args);
  } else {
    console.log("Event %s: %s", event, JSON.stringify(args));
  }
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
/**
 * Module dependencies
 */
var EventEmitter = require('events').EventEmitter;
var dateformat = require('dateformat');
var debug = require('debug')('nmq:queue');

/**
 * Queue status constants
 */

var QueueState = {
  STATE_NEW: 0,
  STATE_SHIFTED: 1,
  STATE_PARTIAL: 2,
  STATE_FINISHED: 3,
  STATE_ERROR: 4
};

/**
 * Queue
 */
var Queue = module.exports = function Queue(db, options) {
  EventEmitter.call(this);
  this._tasks = {};
  this._topics = [];
  this._pausedTasks = [];
  this.delay = 1;
  this.serial = options.serial || false;
  this._state = 'stopped';
  if (options.maxWorkers > 0) {
    this._maxWorkers = options.maxWorkers;
  } else {
    this._maxWorkers = 2;
  }
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

  this.ensureIndexes();
};

/**
 * Inherits `EventEmitter`
 */
Queue.prototype.__proto__ = EventEmitter.prototype;

Queue.prototype.ensureIndexes = function () {
  this.queColl.ensureIndex([
    ['topic', 1],
    ['state', 1]
  ]);
  // index for `results`
  this.resultColl.ensureIndex([
    ['date', 1],
    ['topic', 1],
    ['state', 1],
    ['created', -1],
    ['shifted', -1],
    ['finished', -1]
  ]);
};

/**
 * Push a message to a topic queue with optional options.
 *
 * @param topic {String}
 * @param message {Object}
 * @param [options] {Object}
 */
Queue.prototype.push = function (topic, message, options) {
  var self = this;

  function saveMsg(defer, nextIdDoc) {
    var qid = nextIdDoc.currentId;
    var date = new Date();
    var msg = {_id: qid, topic: topic, message: message,
      state: QueueState.STATE_NEW,
      date: dateformat(date, 'yyyymmdd'),
      created: date
    };
    if (options) {
      if (!isNaN(options.priority)) {
        msg.state = options.priority > 0 ? options.priority * -1 : options.priority;
      }
    }
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
 * @param topic {String} Topic of the queue
 * @param name {String} Name of the task
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
  delete message._id;
  // upsert the message into `toColl`
  return toColl.update({_id: id}, {$set: message}, {safe: true, upsert: true})
    .and(function (defer) {
      // remove the old one from `from`
      fromColl.remove({_id: id}, {safe: true})
        .then(function () {
          message._id = id;
          debug('Message "%s" "%s" moved from "%s" to "%s".', message.topic, id, fromColl.name, toColl.name);
          defer.next(message);
        }).fail(defer.error);
    }).fail(this._onFault(this, true));
};

Queue.prototype._getTopic = function () {
  var idx = getRandomInt(0, this._topics.length - 1);
  return this._topics[idx];
};

Queue.prototype._shift = function () {
  if (this._state === 'stopped' || this._currentWorkers++ >= this._maxWorkers) {
    return;
  }
  var self = this;
  // get messages we are interested
  this.queColl.findAndModify({topic: self._getTopic(), state: {$lte: QueueState.STATE_NEW}},
    // order by priority
    [['state', 1]],
    // modify the message status
    {$set: {shifted: new Date(), state: QueueState.STATE_SHIFTED}},
    // return updated message
    {remove: false, 'new': true, upsert: false}).then(function (message) {
      if (!message) {
        tryNext();
        return;
      }
      debug('Message "%s" shifted from queue.', message._id);
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
    state: QueueState.STATE_INPROGRESS
  };
  debug('Processing task "%s" "%s" "%s".', log.topic, log._id, log.name);
  try {
    task.fn.call(message, message.message, function (taskErr, result) {
      log.finished = new Date();
      if (taskErr) {
        // record error
        log.state = QueueState.STATE_ERROR;
        log.error = taskErr.stack || taskErr.message || taskErr;
        errors[task.name] = taskErr;
        self._log(log, message, done);
        debug('Task "%s" "%s" "%s" error: %s.', log.topic, log._id, log.name, log.error);
      } else {
        log.state = QueueState.STATE_FINISHED;
        // record result if any
        if ('undefined' !== typeof result) {
          results[task.name] = result;
          log.result = result;
        }
        self._log(log, message, done);
        debug('Task "%s" "%s" "%s" finished: %s.', log.topic, log._id, log.name, log.result || '(no result)');
      }
    });
  } catch (e) {
    log.state = QueueState.STATE_ERROR;
    log.error = e.stack || e;
    log.finished = new Date();
    self._log(log, message, done);
    debug('Task "%s" "%s" "%s" exception: %s.', log.topic, log._id, log.name, log.error);
  }
};

Queue.prototype._process = function (message) {
  var topic = message.topic;
  var tasks = this._tasks[topic];
  var self = this;
  var count = 0;
  var results = {};
  var errors = {};
  debug('Processing message %s for topic %s.', message._id, topic);
  function checkFinish() {
    if (++count === tasks.length) {
      if (Object.keys(errors).length > 0) {
        // errors found
        self.resultColl
          .update({_id: message._id}, {$set: {state: QueueState.STATE_PARTIAL}}, {safe: true})
          .then(function () {
            self._emitOrLog('finished', [topic, errors, results]);
            // shift next
            self._currentWorkers--;
            self._shift();
          });
      } else {
        self._emitOrLog('finished', [topic, errors, results]);
        // shift next
        self._currentWorkers--;
        self._shift();
      }
    }
  }

  // filter paused tasks
  if (this._pausedTasks.length > 0) {
    tasks = tasks.filter(function (task) {
      return self._pausedTasks.indexOf(topic + '-' + task.name) === -1;
    });
  }

  if (this.serial) {
    var len = tasks.length;
    var _runTask = function () {
      if (count < len) {
        self._runTask(tasks[count], message, errors, results, function () {
          checkFinish();
          _runTask();
        });
      } else {
        checkFinish();
      }
    };

    _runTask();
  } else {
    // run each tasks in parallel
    tasks.forEach(function (task) {
      self._runTask(task, message, errors, results, checkFinish);
    });
  }
};

Queue.prototype._log = function (log, message, callback) {
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
    .update({_id: log._id}, {$set: setDoc, $push: pushDoc}, {upsert: true, safe: true})
    .fail(this._onFault(this));
  if (log.error) {
    var self = this;
    deferred.then(function () {
      self._emitOrLog('error', [log, message]);
    });
  }
  if ('function' === typeof callback) {
    deferred.then(callback);
  }
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
    debug('Event "%s": %s', event, JSON.stringify(args));
  }
};

Queue.prototype.start = function (workers) {
  debug('Start processing queue.');
  this._state = 'running';
  if (workers) {
    this._maxWorkers = workers;
  }
  for (var i = 0; i < this._maxWorkers; i++) {
    this._shift();
  }
};

Queue.prototype.stop = function () {
  debug('Stop processing queue.');
  this._state = 'stopped';
};

Queue.prototype.close = function () {
  this.stop();
  this.db.close();
};

/**
 * Pause processing a topic or task under that topic.
 *
 * @param topic {String} Topic that you want to pause processing.
 * @param [task] {String} Specific task for the topic you want to pause.
 * @public
 */

Queue.prototype.pause = function (topic, task) {
  if (task) {
    topic += '-' + task;
    if (-1 === this._pausedTasks.indexOf(topic)) {
      this._pausedTasks.push(topic);
    }
  } else {
    this._topics = this._topics.filter(function (t) {
      return topic !== t;
    });
  }
  return this;
};

/**
 * Resume processing a topic or task under that topic.
 *
 * @param topic {String} Topic that you want to resume processing.
 * @param [task] {String} Specific task for the topic you want to resume.
 * @public
 */

Queue.prototype.resume = function (topic, task) {
  if (task) {
    topic += '-' + task;
    this._pausedTasks = this._pausedTasks.filter(function (task) {
      return task !== topic;
    });
  } else {
    if (-1 === this._topics.indexOf(topic)) {
      this._topics.push(topic);
    }
  }
  return this;
};

Queue.QueueState = QueueState;

function nextQueueId(coll) {
  return coll.findAndModify({_id: 'nextQueueId'}, [], {$inc: {currentId: 1}}, {'new': true, upsert: 'true'});
}

function getRandomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1) + min);
}

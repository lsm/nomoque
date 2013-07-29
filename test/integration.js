var assert = require('assert');
var nomoque = require('../index');
var dbSettings = {dbHost: '127.0.0.1', dbPort: 27017, dbPoolSize: 2, dbName: 'nmq_test_db'};
var QueueState = nomoque.Queue.QueueState;
var dateformat = require('dateformat');


describe('Queue', function () {
  this.timeout(10000);

  it('should create an instance of Queue', function () {
    var queue = nomoque.createQueue(dbSettings);
    assert.equal(true, queue instanceof nomoque.Queue);
    queue.close();
  });

  it('should push message to queue and trigger the `queued` event', function (done) {
    var queue = nomoque.createQueue(dbSettings);
    queue.once('queued', function (doc) {
      assert.equal('topicA', doc.topic);
      assert.equal(1, doc.message.a);
      assert.equal(2, doc.message.b);
      assert.equal(dateformat(new Date, 'yyyymmdd'), doc.date);
      assert.equal(nomoque.STATE_NEW, doc.state);
      queue.once('queued', function (doc) {
        assert.equal('topicB', doc.topic);
        assert.equal(null, doc.message);
        queue.close();
        done();
      });
      queue.push('topicB', null);
    });
    queue.push('topicA', {a: 1, b: 2});
  });

  it('should process the queue and emit correponding events', function (done) {
    var worker = nomoque.createQueue(dbSettings);
    var topicAFinished = false;
    var count = 0;
    worker.on('finished', function (topic, errors, results) {
      // `finished` will be emitted even if a task is finished with error
      // (like `topicB`)
      if (++count === 1) {
        assert.equal('topicA', topic);
        assert.equal(3, results['add']);
      } else if (count === 2) {
        worker.close();
        done();
      }
    });
    worker.on('error', function (error, message) {
      // order is important, `topicA` should be finished already
      assert.equal(true, topicAFinished);
      assert.equal('topicB', message.topic);
      assert.equal('topicB', error.topic);
      assert.equal(nomoque.STATE_ERROR, error.state);
      assert.equal(nomoque.STATE_SHIFTED, message.state);
      assert.equal('test error, message received: null', error.error);
    });
    worker.on('fault', function (err) {
      console.error(err.stack || err);
    });
    worker.process('topicB', 'test null message', function (message, callback) {
      assert.equal(null, message);
      callback('test error, message received: ' + JSON.stringify(message));
    });
    worker.process('topicA', 'add', function (message, callback) {
      worker.resume('topicB');
      assert.equal(1, message.a);
      assert.equal(2, message.b);
      topicAFinished = true;
      callback(null, message.a + message.b);
    });

    worker.pause('topicB');
    worker.start();
  });

});
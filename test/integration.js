var assert = require('assert');
var nomoque = require('../index');
var dbSettings = {host: '127.0.0.1', port: 27017, poolSize: 2, db: 'nmq_test_db'};
var common = require('../lib/common');


module.exports = {
  'test Queue#push': function() {
    var queue = nomoque.createQueue(dbSettings);
    queue.once('queued', function(doc) {
      assert.eql('topicA', doc.topic);
      assert.eql(1, doc.message.a);
      assert.eql(2, doc.message.b);
      assert.eql(common.getDateString(), doc.date);
      assert.eql(common.STATE_NEW, doc.state);
      queue.once('queued', function(doc) {
        assert.eql('topicB', doc.topic);
        assert.eql(null, doc.message);
        queue.close();
      });
      queue.push('topicB', null);
    });
    queue.push('topicA', {a: 1, b: 2});
  },

  'test Worker#task': function() {
    var worker = nomoque.createWorker(dbSettings);
    var topicAFinished = false;
    var count = 0;
    worker.on('finished', function(topic, results) {
      // `finished` will be emitted even if a task is finished with error
      // (like `topicB`)
      if (++count === 1) {
        assert.eql('topicA', topic);
        assert.eql(3, results['add']);
      } else if (count === 2) {
        worker.close();
      }
    });
    worker.on('error', function(error, message) {
      // order is important, `topicA` should be finished already
      assert.eql(true, topicAFinished);
      assert.eql('topicB', message.topic);
      assert.eql('test error, message received: null', error);
    });
    worker.on('fault', function(err) {
      console.error(err.stack || err);
    });
    worker.execute('topicB', 'test null message', function(message, callback) {
      callback('test error, message received: ' + JSON.stringify(message));
    });
    worker.execute('topicA', 'add', function(message, callback) {
      assert.eql(1, message.a);
      assert.eql(2, message.b);
      topicAFinished = true;
      callback(null, message.a + message.b);
    });
    worker.start();
  }
};
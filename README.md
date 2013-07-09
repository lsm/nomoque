NoMoQue [![build status](https://secure.travis-ci.org/lsm/nomoque.png)](http://travis-ci.org/lsm/nomoque)
=======

NoMoQue is a database-centric task queue.
It's written in NodeJS and persistent data in MongoDB. It is extremely convenient when you already used these two technologies in your backend stack. You can run task producer/worker any place where you have access to your MongoDB instance.

## Usage

```javascript

var nmq = require('nomoque');
var nmqOptions = {
  dbHost: '127.0.0.1',
  dbPort: 27017,
  dbName: 'db_name',
  collectionPrefix: 'nmq' // Prefix for all collections used by nomoque. The default is no prefix.
};

// create a queue object
var queue = nmq.createQueue(nmqOptions);

// create a payload object, payload could be any javascript plain object which can be stored in mongodb (e.g. string, object, array etc.)
var payload = {
  title: 'any value',
  count: 20
};

// push a payload into a named queue
queue.push('name of queue', payload);

...

// you can process the queue in other part of your code (separate process or server)
// you must give each task a name and for each queue you can attach as many tasks as you wish
queue.process('name of queue', 'task name', function(payload, done) {
  // do something interesting
  ...
  // if everything ok, call done with no argument
  done();
  // or put error object as the first argument if something goes wrong
  done('error, task unfinished');
});

```
NoMoQue
=======

NoMoQue is a database-centric task queue.
It's written in NodeJS and persistent queue in MongoDB. It is extremely convenient when you already used these two technologies in your backend stack. You can run task producer/worker any where if you have access to your MongoDB instance.

## Quick start

```javascript

var nmq = require('nomoque');
var dbOptions = {
  host: '127.0.0.1',
  port: 27017,
  name: 'db_name'
};

// create a queue object
var queue = nmq.createQueue(dbOptions);

// create a payload object, payload could be any native javascript object which can be stored in mongodb (e.g. string, object, array etc.)
var payload = {
  title: 'any value',
  count: 20
};

// push a payload into a named queue
queue.push('name of queue', payload);

...

// you can process the queue in other part of your code (separate process)
// you must give each task a name and for each queue you can attach as many tasks as you wish
queue.process('name of queue', 'task name', function(payload, done) {
  // do something interesting
  ...
  // if everything ok, call done without arguments
  done();
  // or put error object as the first argument if something goes wrong
  done('error, task unfinished');
});

```
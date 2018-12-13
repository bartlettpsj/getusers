const MongoClient = require('mongodb').MongoClient;
const request = require('request');
const _ = require('lodash');

// ,   throttledRequest = require('throttled-request')(request);

// throttledRequest.configure({
//   requests: 50,
//   milliseconds: 1
// });

const MAX_CONCURRENCY = 5;
const MAX_COUNT = 10000;
const MAX_USER_REQ = 1000;
let count = 0;
let doccount = 0;
let inprogress = 0;
let errcount = 0;
let startedAt = Date.now();
let collection;

// Connection URL
const url = 'mongodb://localhost:27017';
const dbName = 'paul';
const client = new MongoClient(url);

client.connect((error) => {
  if (!error) {
    console.log("Connected successfully to server");
    const db = client.db(dbName);
    collection = db.collection('people');

    for (i=0; i<MAX_CONCURRENCY; i++) {
      dorequest();
    }

  } else {
    console.log('Failed to connect', error);
  }
});

// throttledRequest.on('request', function () {
//   inprogress++;
//   console.log('In progress %d. Elapsed time: %d ms', inprogress, Date.now() - startedAt);
// });

// const sleep = async (ms=1000) => { return setTimeout( Promise.resolve(), ms) };
const timeout = (ms=1000) => new Promise(resolve => setTimeout(resolve, ms)) ;

const dorequest = async () => {
  inprogress++; // use with request
  const reqsize = Math.floor(Math.random() * MAX_USER_REQ);
  request(`https://randomuser.me/api/?results=${reqsize}`, async (error, response, body) => {
    count++;
    inprogress--;

    if (!error) {
      // console.log('done %d,%d', count, inprogress, body);
      try {
        const docs = JSON.parse(body);

        if (docs.error) {
          console.log('Waiting 30 seconds after', docs.error.substr(0,25) + '...');
          await timeout(30000);
        } else {
          const promises = _.map(docs.results, doc => collection.insertOne(doc, {}));
          await Promise.all(promises);

          doccount += promises.length;
          console.log('Written request count %d, doc write count %d inprogress %d', count, doccount, inprogress);
        }

      } catch (e) {
        console.log('error',e)
        errcount++;
      }
    }

    if (inprogress < MAX_CONCURRENCY && (count+inprogress) < MAX_COUNT) {
      dorequest();
    }

    if (count == MAX_COUNT) {
      console.log('Closing Count is', count);
      client.close();
    }
  });
}


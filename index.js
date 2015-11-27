"use strict"

var Worker = require("basic-distributed-computation").Worker;
var MongoClient = require("mongodb").MongoClient;

var collectionMatch = /_collection\/{([a-zA-Z0-9\-\$\^\_\:\~\!\@\#\%\&\*\(\|\)\=\+\[\]\;\,\<\.\>\?\'\"\`]+)}/
class Store extends Worker {
  constructor(parent){
    super("general-mongo-storage", parent);
    this.db = null;
    this.init = new Promise((resolve, reject) => {
      MongoClient.connect(process.env.GENERAL_MONGO_STORAGE_URL, (err, db) => {
        if(err){
          reject(err);
          return;
        }
        this.db = db;
        resolve();
      });
    });
  }

  work(req, inputKey, outputKey){
    this.init.then(() => {
      var inputVal = req.body;
      if(inputKey){
        inputVal = req.body[inputKey];
      }
      var collection = collectionMatch.exec(req.paths[req.currentIdx]);
      if(collection){
        collection = collection[1];
        var collectionCursor = this.db.collection(collection);
        collectionCursor.insert(inputVal, (err, results) => {
          if(err){
            req.status(err).next();
            return;
          }
          req.next();
        });
      } else {
        throw new Error("Collection was not defined");
      }
    }).catch((err) => {
      req.status(err).next();
    })
  }
}

module.exports = Store;

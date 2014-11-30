var S3ArrayWriter = require('./S3ArrayWriter'),
    async = require('async'),
    ObjectID = require('mongodb').ObjectID,
    EventEmitter = require('events').EventEmitter,
    util = require('util');

function CollectionWriter(collection) {
    this.collection = collection;
    this.sent = 0;
    this.recieved = 0;
}

util.inherits(CollectionWriter,EventEmitter);


CollectionWriter.prototype.setDestination = function(bucket, path) {

    this.bucket = bucket;
    this.path = path;
};

CollectionWriter.prototype.onProgress = function(sent, recieved) {
    this.sent += sent;
    this.recieved += recieved;
    this.emit('progress', this.sent, this.recieved);
};

CollectionWriter.prototype.saveIndexes = function(done) {
    var self = this;
    this.collection.indexes(function(err, indexes){

        var dataStream = new S3ArrayWriter();
        dataStream.setDestination(self.bucket, self.path+'.indexes.json.gzip');
        indexes.forEach(dataStream.writeObject,dataStream);

        dataStream.on('progress', self.onProgress.bind(self));
        dataStream.end();

        dataStream.on('error',done);
        dataStream.on('done',done);
    });
};

CollectionWriter.prototype.saveData = function(query, done) {
    var stream = this.collection.find(query).stream();
    var dataStream = new S3ArrayWriter();
    dataStream.setDestination(this.bucket, this.path+'.json.gzip');

    stream.on('data', dataStream.writeObject.bind(dataStream));
    stream.on('end', dataStream.end.bind(dataStream));

    dataStream.on('progress', this.onProgress.bind(this));

    dataStream.on('error', done);
    dataStream.on('done', done);
};

CollectionWriter.prototype.execute = function(query, done) {
    async.series([
        this.saveIndexes.bind(this),
        this.saveData.bind(this, query)
    ], done);
};

module.exports = CollectionWriter;

var zlib = require('zlib'),
    AWS = require('aws-sdk'),
    s3stream = require('s3-upload-stream'),
    EventEmitter = require('events').EventEmitter,
    util = require('util');

/**
 * Write out json objects to a gziped file on an s3 bucket
 * one object per line using a streaming API
 *
 * @constructor
 */
function S3ArrayWriter() {
    this.replacer = null;
    this.client = new s3stream(new AWS.S3());
}

util.inherits(S3ArrayWriter,EventEmitter);


S3ArrayWriter.prototype.setReplacer = function(func) {
    this.replacer = func;
};

S3ArrayWriter.prototype.setDestination = function(bucket, path){

    var self = this;

    this.s3File = this.client.upload({
        "Bucket": bucket,
        "Key": path
    });

    this.s3File.on('error', function(err){
        self.emit('error', err)
    });

    this.s3File.on('part', function (details) {
        self.emit('progress', details.sentSize, details.receivedSize);
    });

    this.s3File.on('uploaded', function (details) {
        self.emit('done');
    });

    this.outputStream = zlib.createGzip();
    this.outputStream.pipe(this.s3File);
    this.count = 0;
};


S3ArrayWriter.prototype.writeObject = function(object) {
    this.outputStream.write(JSON.stringify(object,this.replacer)+'\n');
    this.count++;
};

S3ArrayWriter.prototype.end = function() {
    this.outputStream.end();
};


module.exports = S3ArrayWriter;
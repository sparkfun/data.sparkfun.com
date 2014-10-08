#!/usr/bin/env node

var dotenv = require('dotenv').load(),
    path = require('path'),
    fs = require('fs'),
    exec = require('child_process').exec,
    Meta = require('phant-meta-mongodb'),
    Storage = require('phant-stream-mongodb'),
    fromCsv = require('csv-streamify'),
    Keychain = require('phant-keychain-hex');

var reg = new RegExp('stream.csv');

var keys = Keychain({
  publicSalt: process.env.PHANT_PUBLIC_SALT || 'public salt',
  privateSalt: process.env.PHANT_PRIVATE_SALT || 'private salt',
  deleteSalt: process.env.PHANT_DELETE_SALT || 'delete salt'
});

var meta = Meta({
  url: process.env.PHANT_MONGO_URL || 'mongodb://localhost/phant'
});

var storage = Storage({
  cap: process.env.PHANT_CAP || 50 * 1024 * 1024, // 50mb
  url: process.env.PHANT_MONGO_URL || 'mongodb://localhost/phant'
});

meta.each(function(err, stream) {

  var dir = path.join(
    process.env.PHANT_STORAGEDIR || 'tmp',
    stream.id.slice(0, 4),
    stream.id.slice(4)
  );

  fs.exists(path.join(dir, 'stream.csv'), function(exists) {

    if(! exists) {
      return;
    }

    var command = 'cat ' + path.join(dir, 'headers.csv') + ' > ' + 
                  path.join(dir, 'join.csv') + ' && cat $(ls -v ' + path.join(dir, 'stream.csv') +
                  '*) >> ' + path.join(dir, 'join.csv');

    exec(command, function(err) {

      if(err) {
        return console.log('join error: ' + err + ' ' + stream.id);
      }

      var read = fs.createReadStream(path.join(dir, 'join.csv')),
          writer = storage.writeStream(stream.id);

      read.pipe(fromCsv({objectMode: true, columns: true})).pipe(writer);

      read.once('end', function() {
        console.log(stream.id);
      });

    });

  });

});

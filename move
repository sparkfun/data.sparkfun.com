#!/usr/bin/env node

var dotenv = require('dotenv').load(),
    path = require('path'),
    fs = require('fs'),
    execSync = require('exec-sync'),
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

var streams = [],
    i = 0;

meta.list(function(err, s) {

  // no streams found
  if(err || ! s.length) {
    console.log('No streams found');
    process.exit(1);
  }

  streams = s;

  console.log('moving ' + streams.length);

  move(move);

}, {}, 0, 10000);

function move(cb) {

  if(i >= streams.length) {
    console.log('DONE.');
    process.exit();
  }

  var dir = path.join(
    process.env.PHANT_STORAGEDIR || 'tmp',
    streams[i].id.slice(0, 4),
    streams[i].id.slice(4)
  );

  console.log(dir);

  if(fs.existsSync(path.join(dir, 'stream.csv'))) {
    console.log(i + ' does not exist');
    i++;
    cb(move);
    return;
  }

  var command = 'cat ' + path.join(dir, 'headers.csv') + ' > ' + 
                path.join(dir, 'join.csv') + ' && cat $(ls -v ' + path.join(dir, 'stream.csv') +
                '*) >> ' + path.join(dir, 'join.csv');

  try {

    execSync(command);

    var read = fs.createReadStream(path.join(dir, 'join.csv')),
        writer = storage.writeStream(streams[i].id);

    read.pipe(fromCsv({objectMode: true, columns: true})).pipe(writer);

    writer.on('finish', function() {
      console.log(i + ' moved');
      i++;
      cb(move);
    });

  } catch(e) {
    console.log(i + ' ERROR: ' + e);
    i++;
    cb(move);
  }

}

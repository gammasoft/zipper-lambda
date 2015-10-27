'use strict';

var Aws = require('aws-sdk'),
    async = require('async'),

    fs = require('fs'),
    path = require('path'),
    childProcess = require('child_process');

function processJob(job) {
    console.log('started processing job');

    var filesSize = 0,
        temporaryDirectoryPath = path.join(__dirname, 'files'),
        compressedFilePath,
        compressedFileSize,
        uploadedFileLocation,
        s3client = new Aws.S3({
            endpoint: 'https://s3-' + job.credentials.region + '.amazonaws.com',
            s3BucketEndpoint: false,

            accessKeyId: job.credentials.accessKeyId,
            secretAccessKey: job.credentials.secretAccessKey
        });

    job.files = job.files.map(function(key) {
        var key = key.split('/'),
            file = {
                fullKey: key.join('/'),
                bucket: key.shift(),
                key: key.join('/')
            };

        file.name = path.basename(file.key);
        return file;
    });

    job.destination = job.destination.split('/');
    job.destination = {
        fullKey: job.destination.join('/'),
        bucket: job.destination.shift(),
        key: job.destination.join('/')
    }

    job.destination.name = path.basename(job.destination.key);

    function validateFile(header, cb) {
        var size = parseInt(header.ContentLength, 10);
        // TODO: Add max file size validation

        filesSize += size;
        cb();
    }

    function getHeaders(cb) {
        async.eachSeries(job.files, function(file, cb) {
            console.log('downloading headers from %s', file.fullKey);

            s3client.headObject({
                Bucket: file.bucket,
                Key: file.key
            }, function(err, header) {
                if(err) {
                    cnosole.log('error obtaining file header');
                    return cb(err);
                }

                validateFile(header, cb);
            });
        }, function(err) {
            if(err) {
                console.log('error downloading headers');
                return cb(err);
            }

            // TODO: Add max total size validation
            console.log('total size to compress is %s', filesSize);
            cb();
        });
    }

    function createFilesFolder(cb) {
        var mkdir = childProcess.spawn('mkdir', [
            temporaryDirectoryPath
        ], {
            cwd: __dirname
        });

        mkdir.on('close', function(exitCode) {
            if(exitCode !== 0) {
                console.log('error creating files folder - mkdir exited with code %s', exitCode);
                return cb(new Error('mkdir exited with code: ' + exitCode));
            }

            console.log('files folder created');
            cb();
        });
    }

    function downloadFiles(cb) {
        console.log('downloading %s files', job.files.length);

        async.eachSeries(job.files, function(file, cb) {
            console.log('downloading file %s', file.fullKey);

            var fileDownload = s3client.getObject({
                Bucket: file.bucket,
                Key: file.key
            }).createReadStream();

            var filePath = path.join(temporaryDirectoryPath, file.name),
                writeStream = fs.createWriteStream(filePath);

            fileDownload.pipe(writeStream);

            var bytesReceived = 0;
            fileDownload.on('data', function(chunk) {
                bytesReceived += chunk.length;
                console.log('received %s', bytesReceived);
            });

            fileDownload.on('end', function() {
                console.log('download completed');
                // zipFile(filePath, cb);
                cb();
            });
        }, function(err) {
            if(err) {
                console.log('error downloading files');
                return cb(err);
            }

            compressedFilePath = path.join(temporaryDirectoryPath, job.destination.name);

            console.log('all downloads completed');
            cb();
        });
    }

    function createCompressedFile(cb) {
        console.log('creating compressed file');

        var zip = childProcess.spawn('zip', [
            '-r',
            job.destination.name,
            './'
        ], {
            cwd: temporaryDirectoryPath
        });

        zip.stdout.on('data', function(data) {
            console.log(data.toString().trim());
        });

        zip.stderr.on('data', function() {
            console.log( data.toString().trim());
        });

        zip.on('close', function(exitCode) {
            if(exitCode !== 0) {
                console.log('error creating compressed file - zip exited with code %s', exitCode);
                cb(new Error('zip exited with code: ' + exitCode));
            }

            compressedFilePath = path.join(temporaryDirectoryPath, job.destination.name);
            console.log('compressed file created');
            cb();
        });
    }

    function getCompressedFileSize(cb) {
        console.log('getting compressed file size');

        fs.stat(compressedFilePath, function(err, stats) {
            if(err) {
                console.log('error getting compressed file size');
                return cb(err);
            }

            compressedFileSize = stats.size;
            var compressionEfficiency = ((1 - compressedFileSize/filesSize) * 100).toFixed(2);

            console.log('compressed file size is %s', compressedFileSize);
            console.log('original file was compressed by %s%', compressionEfficiency);

            cb();
        });
    }

    function uploadCompressedFile(cb) {
        console.log('uploading compressed file to %s', job.destination.fullKey);

        var upload = s3client.upload({
                Bucket: job.destination.bucket,
                Key: job.destination.key,
                ACL: job.acl || 'private',
                StorageClass: job.storageClass || 'STANDARD',
                Body: fs.createReadStream(compressedFilePath)
            });

        upload.on('httpUploadProgress', console.log);

        upload.send(function(err, data) {
            if(err) {
                console.log('error uploading file');
                return cb(err);
            }

            uploadedFileLocation = data.Location;
            console.log('file available at %s', uploadedFileLocation);

            cb();
        });
    }

    function sendNotifications(cb) {
        if(!job.notifications || !job.notifications.length) {
            console.log('no notifications to send');
            return cb();
        }

        console.log('sending %s notifications', job.notifications.length);
        async.eachSeries(job.notifications, function(notification, cb) {
            var notificationType = notification.type.toLowerCase(),
                notificationStrategy = notificationTypes[notificationType];

            if(!notificationStrategy) {
                console.log('unkown notification type "%s"', notificationType);
                return cb();
            }

            notificationStrategy({
                job: job,
                notification: notification,
                results: {
                    location: uploadedFileLocation,
                    size: compressedFileSize,
                    status: 'success'
                }
            }, cb);
        }, cb);
    }

    var startTime = new Date();

    async.series([
        getHeaders,
        createFilesFolder,
        downloadFiles,
        createCompressedFile,
        getCompressedFileSize,
        uploadCompressedFile,
        sendNotifications
    ], function(err) {
        if(err) {
            // TODO: Send fail notification

            console.log('error processing job');
            console.log(err);
        }

        var jobTime = new Date() - startTime;
        console.log('job completed in %s', jobTime);
    });
}

module.exports.handler = processJob;
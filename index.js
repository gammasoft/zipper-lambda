'use strict';

var Aws = require('aws-sdk'),
    async = require('async'),

    fs = require('fs'),
    path = require('path'),
    childProcess = require('child_process'),

    notificationTypes = require('./notifications');

function processJob(event, context) {
    if(event.auth !== process.env['SECRET_TOKEN']) {
        console.log('auth failed with token: ' + event.auth);
        return context.fail();
    }

    console.log('started processing job');

    var filesSize = 0,
        job = event.data,
        isLambda = context.awsRequestId,
        temporaryDirectoryPath,
        compressedFilePath,
        compressedFileSize,
        uploadedFileLocation,
        s3client = new Aws.S3({
            endpoint: 'https://s3-' + job.credentials.region + '.amazonaws.com',
            s3BucketEndpoint: false,

            accessKeyId: job.credentials.accessKeyId,
            secretAccessKey: job.credentials.secretAccessKey
        });

    if(isLambda) {
        temporaryDirectoryPath = path.join('/tmp', context.awsRequestId);

        process.env['PATH'] = [
            process.env['PATH'],
            process.env['LAMBDA_TASK_ROOT']
        ].join(':');
    } else {
         temporaryDirectoryPath = path.join(__dirname, 'files');
    }

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

    function createFilesFolder(cb) {
        console.log('creating files folder at %s', temporaryDirectoryPath);

        var mkdir = childProcess.spawn('mkdir', [
            temporaryDirectoryPath
        ], {
            cwd: __dirname
        });

        mkdir.stdout.on('data', function(data) {
            console.log(data.toString().trim());
        });

        mkdir.stderr.on('data', function(data) {
            console.log(data.toString().trim());
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

        async.eachLimit(job.files, 50, function(file, cb) {
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
                filesSize += bytesReceived;
                console.log('received %s', bytesReceived);
            });

            fileDownload.on('end', function() {
                console.log('download completed');
                cb();
            });
        }, function(err) {
            if(err) {
                console.log('error downloading files');
                return cb(err);
            }

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

        zip.stderr.on('data', function(data) {
            console.log(data.toString().trim());
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
            var compressionEfficiency = ((1 - compressedFileSize / filesSize) * 100).toFixed(2);

            console.log('compressed file size is %s', compressedFileSize);
            console.log('original files were compressed by %s%', compressionEfficiency);

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
        async.eachLimit(job.notifications, 10, function(notification, cb) {
            var notificationType = notification.type.toLowerCase(),
                notificationStrategy = notificationTypes[notificationType];

            if(!notificationStrategy) {
                console.log('unkown notification type "%s" - skipping', notificationType);
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
            }, function(err) {
                if(err) {
                    console.log('failed to send notification:');
                    console.log(JSON.stringify(notification, null, 4));
                    console.log(err);
                }

                return cb();
            });
        }, cb);
    }

    var startTime = new Date();

    async.series([
        createFilesFolder,
        downloadFiles,
        createCompressedFile,
        getCompressedFileSize,
        uploadCompressedFile,
        sendNotifications
    ], function(err) {
        var jobTime = new Date() - startTime;
        console.log('job completed in %s', jobTime);

        if(err) {
            // TODO: Send fail notifications
            console.log('job processing failed');
            console.log(err);
            return context.fail();
        }

        context.succeed();
    });
}

module.exports.handler = processJob;
'use strict';

var request = require('request');

module.exports = function httpNotification(options, callback) {
    var notification = options.notification,
        results = options.results,
        job = options.job;

    console.log('sending HTTP notification to "%s %s"', notification.method.toUpperCase(), notification.url);

    if(typeof notification.strictSSL === 'undefined') {
        notification.strictSSL = true;
    }

    request({
        method: notification.method,
        url: notification.url.replace(/{:id}/g, job.id),
        strictSSL: notification.strictSSL,
        json: {
            id: job.id,
            status: results.status,
            location: results.location,
            size: results.size
        }
    }, function(err, res, body) {
        if(err) {
            console.log('error sending HTTP notification');
            return callback(err);
        }

        console.log('notification sent, status code: %s', res.statusCode);
        if(res.statusCode === 200) {
            console.log('response was successful, response body:');
            console.log(body);
        }

        callback(null, res.statusCode);
    });
};
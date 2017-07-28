/*
* import-wxt520
* https://github.com/52North/ConnectinGEO
*
* Copyright (c) 2016 matthesrieke
* Licensed under the MIT license.
*/

'use strict';

var asyncd = require('async');
var request = require('request-promise');
var moment = require('moment');
var fs = require("fs");

var insertTemplate = require('./insert-template.json');
var batchTemplate = require('./batch-template.json');
var sensorTemplate = fs.readFileSync('lib/insert-sensor.xml', "utf8");

var doImport = function(config) {
  var requestList = [];
  var insertSensorRequests = [];

  asyncd.eachLimit(config.series, 1, function(ser, done) {
    var target = ser.dataUrl+'&timespan='+encodeURIComponent(config.timePeriod);
    console.info(ser.observedProperty+': '+target);

    request(target).then(function (response) {
      var data = JSON.parse(response);
      var offering = config.codespace + (ser.procedure ? ser.procedure.split(" ").join("_") : config.procedure);

      var tempInserts = [];

      if (ser.procedure) {
        /*
        * insert sensor preparation
        */
        var sensor = sensorTemplate
        .split("${procedure}").join(ser.procedure)
        .split("${offering}").join(offering)
        .split("${featureOfInterest}").join(config.featureOfInterest)
        .split("${observedProperty}").join(ser.observedProperty)
        .split("${uom}").join(ser.uom)
        .split("${lat}").join(config.coordinates[0])
        .split("${lon}").join(config.coordinates[1])
        .split("${alt}").join((config.altitude || '-INF')+'');
        insertSensorRequests.push(sensor);
      }

      /*
      * go through the time series data
      */
      for (var key in data) {
        if (data.hasOwnProperty(key)) {
          for (var i = 0; i < data[key].values.length; i++) {
          // for (var i = 0; i < 1; i++) {
            var dateTime = moment(data[key].values[i][0]);
            var val = data[key].values[i][1];

            var template = JSON.parse(JSON.stringify(insertTemplate));
            template.offering = offering;
            // template.observation[0].identifier.value = config.procedure+'/'+ser.observedProperty+'/1';
            template.observation[0].procedure = ser.procedure || config.procedure;
            template.observation[0].observedProperty = ser.observedProperty;
            template.observation[0].featureOfInterest.identifier.value = config.featureOfInterest;
            template.observation[0].featureOfInterest.name[0].value = config.featureOfInterest;
            template.observation[0].featureOfInterest.sampledFeature = ['http://www.opengis.net/def/nil/OGC/0/unknown'];
            template.observation[0].featureOfInterest.geometry.coordinates = config.coordinates;
            if (config.altitude) {
              template.observation[0].featureOfInterest.geometry.coordinates[2] = config.altitude;
            }
            template.observation[0].phenomenonTime = dateTime.toISOString();
            template.observation[0].resultTime = dateTime.toISOString();
            template.observation[0].result = {
              'uom': ser.uom,
              'value': val
            };

            /*
            * add to internal batch list
            */
            tempInserts.push(template);
          }
        }
      }

      /*
      * add batch to overall request list
      */
      var batch = JSON.parse(JSON.stringify(batchTemplate));
      batch.requests = tempInserts;
      requestList.push(batch);

      console.info('total count: '+requestList.length);
      done();
    }).catch(function (err) {
      console.warn(err);
      done();
    });

  }, function(err) {
    /*
    * do all requests as POST
    */
    console.info('Pushing to SOS!');
    if (err) {
      console.warn(err);
      return;
    }

    var uri = 'http://localhost:8080/52n-sos-webapp/service';
    var finalRequestOptions = [];

    /*
    * insert sensor requests
    */
    for (var i = 0; i < insertSensorRequests.length; i++) {
      finalRequestOptions.push({
        method: 'POST',
        uri: uri,
        body: insertSensorRequests[i],
        headers: {
          'content-type': 'application/soap+xml'
        }
      });
    }

    /*
    * insert observation requests
    */
    for (var i = 0; i < requestList.length; i++) {
      finalRequestOptions.push({
        method: 'POST',
        uri: uri,
        body: requestList[i],
        json: true
      });
    }

    /*
    * only one request at a time
    */
    var counter = 0;
    asyncd.eachLimit(finalRequestOptions, 1, function(o, done2) {
      request(o).then(function (sosResp) {
        console.info(JSON.stringify(sosResp));
        console.info(counter + ' request done!');
      })
      .catch(function (err) {
        console.warn(counter + ' request failed!');
        console.error(JSON.stringify(err, null, 4));
      }).finally(function() {
        counter++;
        done2();
      });
    }, function(err) {
      if (err) {
        console.warn('Could not push all requests');
        console.warn(err);
      }
      else {
        console.info('all requests pushed!');
      }
    });

  });


};


exports.doImport = doImport;

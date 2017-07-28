var pg = require('pg');

var phenomenons = require('./utils/phenomenons');
var tracks = require('./track-mongo-provider');

//Disable automatic date parsing by node-postgress and parse dates in your application:
// pg.types.setTypeParser(1114, function(stringValue) {
//     console.log(stringValue);
//     return new Date(Date.parse(stringValue + "+0000"));
// });

var user = process.env.POSTGRES_USER;
var password = process.env.POSTGRES_PASSWORD;
var database = process.env.POSTGRES_DB;

if (!user) {
    user = 'postgres';
}
if (!password) {
    password = '1234';
}
if (!database) {
    database = 'postgres'
}

var host = 'postgres';
var port = '5432';

var client = new pg.Client({
    user: user,
    password: password,
    host: host,
    port: port,
    database: database,
});

client.connect();

// 1) featureofinteresttype
client.query("INSERT INTO public.featureofinteresttype values (nextval('public.featureofinteresttypeid_seq'), 'http://www.opengis.net/def/samplingFeatureType/OGC-OM/2.0/SF_SamplingCurve')", function(err) {});

// 2) observationtype
client.query("INSERT INTO public.observationtype values (nextval('public.observationtypeid_seq'), 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement')", function(err) {});

// 3) proceduredescriptionformat
client.query("INSERT INTO public.proceduredescriptionformat values (nextval('public.procdescformatid_seq'), 'http://www.opengis.net/sensorml/2.0')", function(err) {});

// FOR EACH
phenomenons.variables.forEach(function(phenomenon) {
    // 4) unit
    client.query("INSERT INTO public.unit (unitid, unit) (SELECT nextval('public.unitid_seq'), $1)", [phenomenon.unit], function(err) {});

    // 5) observableProperty
    client.query("INSERT INTO public.observableproperty (observablepropertyid, identifier, codespace, name, codespacename, description, disabled) (SELECT nextval('public.observablepropertyid_seq'), $1, null, $1, null, null, 'F')", [phenomenon.name], function(err) {});
});

var t = 0;

tracks.findAllTracksObs
    .take(15)
    .doOnNext(function(track) {
        console.log("loading track " + t++);

        // 6) featureofinterst
        console.log("feautreofinterest");

        var measurements = track.measurements;
        var lineString = "LINESTRING("
        measurements.forEach(function(m) {
            lineString += m.geometry.coordinates[1] + " " + m.geometry.coordinates[0] + ", ";
        });
        lineString = lineString.substring(0, lineString.length - 2) + ")";

        client.query("INSERT INTO public.featureofinterest (featureofinterestid, featureofinteresttypeid, identifier, codespace, name, codespacename,  description,  geom,  descriptionxml,  url) (SELECT nextval('public.featureofinterestid_seq'), 1, $1, null, null, null, null, ST_GeomFromText($2, 4326), null, null)", [track._id.toString(), lineString], function(err) {});
    })
    .doOnNext(function(track) {
        // 7) procedure
        console.log("procedure");

        var sensor = track.sensor;
        var sensorProp = sensor.properties;
        var name = sensorProp.manufacturer + " " + sensorProp.model + " " + sensorProp.constructionYear + " " + sensorProp.fuelType;

        client.query("INSERT INTO public.procedure (procedureid, hibernatediscriminator, proceduredescriptionformatid, identifier, codespace, name, codespacename, description, deleted, disabled, descriptionfile, referenceflag, typeof, istype, isaggregation, mobile,insitu) (SELECT nextval('procedureid_seq'), 'F', 1, $1, null::bigint, $2, null::bigint, null, 'F', 'F', null, 'F', null::bigint, 'F', 'F', 'T', 'F')", [sensor._id.toString(), name], function(err) {});
    })
    .doOnNext(function(track) {
        // 8) offering
        console.log("offering");

        var sensor = track.sensor;
        var sensorProp = sensor.properties;
        var name = sensorProp.manufacturer + " " + sensorProp.model + " " + sensorProp.constructionYear + " " + sensorProp.fuelType;

        client.query("INSERT INTO public.offering (offeringid, hibernatediscriminator, identifier, codespace, name, codespacename, description, disabled) (SELECT nextval('offeringid_seq'), 'F', $1, null, $2, null, null, 'F')", [sensor._id.toString(), name], function(err) {});
    })
    .doOnNext(function(track) {
        // 10) series
        console.log("series");

        var measures = track.measurements;
        var begin = measures[0].time;
        var end = measures[measures.length - 1].time;

        track.measurements.forEach(function(m) {
            m.phenomenons.forEach(function(p) {
                client.query("INSERT INTO public.series (seriesid, featureofinterestid, observablepropertyid, procedureid, offeringid, deleted, published, firsttimestamp, lasttimestamp, firstnumericvalue, lastnumericvalue, unitid, seriestype) (SELECT nextval('public.seriesid_seq'), (SELECT featureofinterestid FROM featureofinterest WHERE identifier = $1), (SELECT observablepropertyid FROM observableproperty WHERE identifier = $2), (SELECT procedureid FROM procedure WHERE identifier = $3), (SELECT offeringid FROM offering WHERE identifier = $3),  'F', 'T', $4, $5, null, null, (SELECT unitid FROM unit WHERE unit = $6), 'measurement')", [track._id.toString(), p.phen._id, track.sensor._id.toString(), begin, end, p.unit], function(err) {});
                // client.query("INSERT INTO public.series (seriesid, featureofinterestid, observablepropertyid, procedureid,             deleted, published, firsttimestamp, lasttimestamp, firstnumericvalue, lastnumericvalue, unitid, seriestype) (SELECT nextval('public.seriesid_seq'), (SELECT featureofinterestid FROM featureofinterest WHERE identifier = $1), (SELECT observablepropertyid FROM observableproperty WHERE identifier = $2), (SELECT procedureid FROM procedure WHERE identifier = $3),                                                                     'F', 'T', $4, $5, null, null, (SELECT unitid FROM unit WHERE unit = $6), 'measurement')", [track._id.toString(), p.phen._id, track.sensor._id.toString(), begin, end, p.unit], function(err) {});
            });
        });
    })
    .doOnNext(function(track) {
        // 11) observation and numeric value
        console.log("observation and numeric value");

        track.measurements.forEach(function(m) {
            m.phenomenons.forEach(function(p) {

                client.query("INSERT INTO public.observation (observationid, seriesid, phenomenontimestart, phenomenontimeend, resulttime, identifier, codespace, name, codespacename, description, deleted, validtimestart, validtimeend, unitid, samplinggeometry) (SELECT nextval('public.observationid_seq'), ( SELECT seriesid FROM SERIES s JOIN featureofinterest f ON s.featureofinterestid = f.featureofinterestid JOIN observableproperty op ON s.observablepropertyid = op.observablepropertyid JOIN procedure p ON s.procedureid = p.procedureid JOIN offering off ON s.offeringid = off.offeringid WHERE f.identifier = $1 AND op.identifier = $2 AND p.identifier = $3 AND off.identifier = $3), $4, $4, $5, null, null, null, null, null, 'F', null, null, (SELECT unitid FROM unit WHERE unit = $6), ST_GeomFromText('POINT('||$7::text||' '||$8::text||')', 4326))", [track._id.toString(), p.phen._id, track.sensor._id.toString(), m.time, m.created, p.phen.unit, m.geometry.coordinates[1], m.geometry.coordinates[0]], function(err) {
                // client.query("INSERT INTO observation (observationid, seriesid, phenomenontimestart, phenomenontimeend, resulttime, identifier, codespace, name, codespacename, description, deleted, validtimestart, validtimeend, unitid, samplinggeometry) (SELECT nextval('public.observationid_seq'), ( SELECT seriesid FROM SERIES s JOIN featureofinterest f ON s.featureofinterestid = f.featureofinterestid JOIN observableproperty op ON s.observablepropertyid = op.observablepropertyid JOIN procedure p ON s.procedureid = p.procedureid                                                    WHERE f.identifier = $1 AND op.identifier = $2 AND p.identifier = $3),                                      $4, $4, $5, null, null, null, null, null, 'F', null, null, (SELECT unitid FROM unit WHERE unit = $6), ST_GeomFromText('POINT('||$7::text||' '||$8::text||')', 4326))", [track._id.toString(), p.phen._id, track.sensor._id.toString(), m.time, m.created, p.phen.unit, m.geometry.coordinates[1], m.geometry.coordinates[0]], function(err) {
                    if (err) console.log(err);
                })
                client.query("INSERT INTO public.numericvalue (observationid, value) (SELECT currval('public.observationid_seq'), $1)", [p.value], function(err) {
                    if (err) console.log(err);
                });
            });
        });
    })
    .doOnNext(function(track) {
        // 13 observationConstellation
        console.log("observationConstellation");
        client.query("INSERT INTO public.observationConstellation (observationconstellationid, observablepropertyid, procedureid, observationtypeid, offeringid, deleted, hiddenchild) (SELECT nextval('public.observationconstellationid_seq'), q.observablepropertyid, q.procedureid, 1, q.offeringid, 'F', 'F' FROM (SELECT DISTINCT p.procedureid, op.observablepropertyid, off.offeringid FROM series s JOIN observableproperty op ON s.observablepropertyid = op.observablepropertyid JOIN procedure p ON s.procedureid = p.procedureid JOIN offering off ON off.identifier = p.identifier) q)", [], function(err) {
            if (err) console.log(err);
        });
    })
    .doOnNext(function(track) {
        // 14 observationhasoffering
        console.log("observationhasoffering");

        client.query("INSERT INTO public.observationhasoffering (observationid, offeringid)(SELECT o.observationid, off.offeringid FROM public.observation o inner join public.series s on o.seriesid = s.seriesid inner join public.procedure p on s.procedureid = p.procedureid inner join offering off on off.identifier  = p.identifier)", [], function(err) {
            if (err) console.log(err);
        });
    })
    .doOnNext(function(track) {
        // 14 Update series table with first/last timestamp as values
        console.log("last step");

        client.query("UPDATE public.series s SET firsttimestamp = r.phenomenonTimestart, firstnumericvalue = r.value FROM (SELECT q.seriesid, nv.value, o.phenomenonTimestart FROM observation o inner join numericvalue nv on o.observationid = nv.observationid, (SELECT seriesid, min(phenomenonTimestart) FROM public.observation GROUP BY seriesid) q WHERE q.seriesid = o.seriesid AND o.phenomenonTimestart = q.min) r WHERE r.seriesid = s.seriesid", [], function(err) {
            if (err) console.log(err);
        });
        client.query("UPDATE public.series s SET lasttimestamp = r.phenomenonTimestart, lastnumericvalue = r.value FROM (SELECT q.seriesid, nv.value, o.phenomenonTimestart FROM observation o inner join numericvalue nv on o.observationid = nv.observationid, (SELECT seriesid, max(phenomenonTimeStart) FROM public.observation GROUP BY seriesid) q WHERE q.seriesid = o.seriesid AND o.phenomenonTimestart = q.max) r WHERE r.seriesid = s.seriesid", [], function(err) {
            if (err) console.log(err);
        });
    })
    .delay(100)
    .subscribe(function(track) {
        console.log("track received");
    });

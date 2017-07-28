var Rx = require('rx');
var mongo = require('mongodb');

var ObjectID = mongo.ObjectID;
var DBRef = mongo.DBRef;

var MongoClient = mongo.MongoClient;

var url = 'mongodb://mongo:27017/enviroCar';

var connect = Rx.Observable.fromNodeCallback(mongo.connect, mongo);
var db$ = connect(url);

// function find() {
//     var req$ = db$.flatMapLatest(function(db) {
//         var collection = db.collection('tracks');
//         var cursor = collection.find({});
//         var query = Rx.Observable.fromNodeCallback(cursor.toArray, cursor);
//         return query();
//     });
//     req$.subscribe(function(results) {
//         console.log(results);
//     });
// }


function findAllTracksObs() {
    var req$ = db$.flatMapLatest(function(db) {
        var collection = db.collection('tracks');
        var cursor = collection.find({});
        var query = Rx.Observable.fromNodeCallback(cursor.toArray, cursor);
        return query();
    });
    return req$;
}

function fetchTrack(track) {
    var req$ = db$.flatMapLatest(function(db) {
        var collection = db.collection('measurements');
        var cursor = collection.find({
            track: new DBRef('tracks', new ObjectID(track._id))
        });

        var query = Rx.Observable.fromNodeCallback(cursor.toArray, cursor);
        return query()
            .map(function(measurements) {
                console.log(measurements.length);
                track.measurements = measurements;
                return track;
            });;
    });
    return req$;
}

exports.findAllTracksObs = findAllTracksObs()
    .concatMap(Rx.Observable.from)
    .concatMap(fetchTrack);

const { prototype } = require("events");
var haversine = require("haversine-distance");
const {parentPort, port, workerData} = require("worker_threads");
// console.log('Message received from main script');
let point1;
let point2;
let distance;

parentPort.on('message', (data) => {
    // console.log(JSON.stringify(data))
    point1 = {lat:parseFloat(data.point1.latitude),lng:parseFloat(data.point1.longitude)}
    point2 = {lat:parseFloat(data.point2.latitude),lng:parseFloat(data.point2.longitude)}
    distance = haversine(point1, point2)
    // console.log('the distance is '+ distance)
    parentPort.postMessage(distance)

});
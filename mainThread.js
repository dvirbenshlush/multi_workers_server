const app = require('express')();
const fs = require('fs');
const results = [];
const csv = require('csv-parser')
const lodash = require('lodash')
const { runThread } = require('./worker.js')
const { isMainThread } = require('worker_threads');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;


let number_of_threads = read_confige_file()


const csvWriter = createCsvWriter({
    path: 'result.csv',
    header: [
        { id: 'row_id', title: 'row_id' },
        { id: 'vehicle_id', title: 'vehicle_id' },
        { id: 'latitude', title: 'latitude' },
        { id: 'longitude', title: 'longitude' },
        { id: 'distance_from_prev_point', title: 'distance_from_prev_point' },
        { id: 'worker_id', title: 'worker_id' },
    ]
});

let line;
let row_id = 0;
if (isMainThread) {
    fs.createReadStream('coordinates_for_node_test.csv').pipe(csv({}))
        .on('data', data => {
            line = { row_id: ++row_id, vehicle_id: data.vehicle_id, latitude: data.latitude, longitude: data.longitude }
            results.push(line)
        }).on('end', () => {
            let worker;
            const result = lodash.chain(results).groupBy(item => item.vehicle_id)
                .map((value, key) => ({ vehicle_id: key, worker_threads: value }))
                .value()

            let groups_of_vehicle = new Map()
            for (let i = 0; i < result.length; i++) {
                groups_of_vehicle.set(result[i].vehicle_id, result[i].worker_threads)
            }

            let sort_array_of_lines = [];
            groups_of_vehicle.forEach((v_id, group) => {
                for (let i = 0; i < v_id.length; i++)
                    sort_array_of_lines.push(v_id[i])
            })
            // console.log(sort_array_of_lines)
            // (----------------------------------------------------------------------------------------------------------------)
            let workers = []
            for (let index = 0; index < number_of_threads; index++) {
                worker = runThread()
                workers.push(worker)
            }
            for (let worker_index = 0; worker_index < number_of_threads; worker_index++) {
                for (let index = 1; index < sort_array_of_lines.length; index++) {
                    worker_id = workers[worker_index].threadId;
                    point1 = { latitude: sort_array_of_lines[index - 1].latitude, longitude: sort_array_of_lines[index - 1].longitude }
                    point2 = { latitude: sort_array_of_lines[index].latitude, longitude: sort_array_of_lines[index].longitude }
                    workers[worker_index].postMessage({ point1: point1, point2: point2 })
                }
                let number_of_lines_written = 1;
                let distance_array = [];
                const initial = {
                    row_id: sort_array_of_lines[number_of_lines_written - 1].row_id, vehicle_id: sort_array_of_lines[number_of_lines_written - 1].vehicle_id, latitude: sort_array_of_lines[number_of_lines_written - 1].latitude,
                    longitude: sort_array_of_lines[number_of_lines_written - 1].longitude, distance_from_prev_point: 0,
                    worker_id: workers[worker_index].threadId
                }
                distance_array.push(initial)
                workers[worker_index].on("message", data => {
                    number_of_lines_written++
                    distance_array.push({
                        row_id: sort_array_of_lines[number_of_lines_written - 1].row_id, vehicle_id: sort_array_of_lines[number_of_lines_written - 1].vehicle_id, latitude: sort_array_of_lines[number_of_lines_written - 1].latitude,
                        longitude: sort_array_of_lines[number_of_lines_written - 1].longitude, distance_from_prev_point: data,
                        worker_id: workers[worker_index].threadId
                    })

                    if (sort_array_of_lines.length == number_of_lines_written) {

                        csvWriter.writeRecords(distance_array)
                    }
                });
            }
        })
}

app.get('/', (req, res) => {
    res.send('send')
})

app.listen(3000, () => {
    console.log('server is running')
})

function parseCsv() {
    let arr = []
    fs.createReadStream('coordinates_for_node_test.csv').pipe(csv({}))
        .on('data', data => {
            line = { row_id: ++row_id, vehicle_id: data.vehicle_id, latitude: data.latitude, longitude: data.longitude }
            arr.push(line)
        })
    return arr
}



function read_confige_file(){
    let number_of_threads;
    try {  
        var data = fs.readFileSync('config_file.txt', 'utf8');
        number_of_threads = parseInt(data.toString().split(':')[1])
    } catch(e) {
        console.log('Error:', e.stack);
    }
    return number_of_threads
}
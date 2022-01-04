const { Worker } = require('worker_threads');

function runThread() {
   
    let worker = new Worker('./calculateDistance.js');

    // registering events in main thread to perform actions after receiving data/error/exit events
    worker.on('message', (msg) => {
        // console.log(msg)
    });

    // for error handling
    worker.on('error',(e)=>{
        console.log(e)
    });

    // for exit
    worker.on('exit', (code) => {
        if(code !== 0) {
            console.error(new Error(`Worker stopped Code ${code}`))
        }
    });
    return worker;
   
  }

  module.exports = { runThread }
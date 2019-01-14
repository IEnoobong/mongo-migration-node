const mongodb = require('mongodb')
const async = require('async')

const url = 'mongodb://localhost:27017'
const dbName = 'edx-course-db'
const collectionName = 'customers'

const customers = require('./m3-customer-data')
const addresses = require('./m3-customer-address-data')

const packetsize = parseInt(process.argv[2], 10) || customers.length

mongodb.connect(url, { useNewUrlParser: true }, (error, client) => {
    if (error) {
        console.error(error);
        return process.exit(1);
    }

    const db = client.db(dbName);

    let mergeTasks = [];

    customers.forEach((customer, index) => {
        let start, limit
        customer = Object.assign(customer, addresses[index])
        if (index % packetsize === 0) {
            start = index
            limit = start + packetsize
            limit = (limit > customers.length) ? customers.length : limit

            mergeTasks.push((callback) => {
                console.log(`Inserting records ${start} to ${limit} of ${customers.length}`)
                db.collection(collectionName).insertMany(customers.slice(start, limit), (error, results) => {
                    callback(error, results);
                })
            });
        }
    });

    async.parallel(mergeTasks, (error, results) => {
        if (error) {
            console.error(error);
        }
        console.log(`Work don finish`)
        client.close()
    })
})
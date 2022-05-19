const fs = require('fs')
const { parse } = require("csv-parse")
const { stringify } = require("csv-stringify")
const _ = require('lodash')

async function Trasks() {
    
    const promiseOrders = () => new Promise((resolve, reject) => {
        const resultsOrders = [];

        fs.createReadStream("../master-files/orders.csv")
            .pipe(parse({ delimiter: ",", from_line: 2 }))
            .on('data', function (data) {
                const orders = data[2].split(' ').join('')
                data.pop()
                resultsOrders.push(Array.from([...data, ...orders], Number))
            })
            .on('end', function () {
                resolve(resultsOrders)
            })
            .on('error', function (err) {
                reject(err);
            });

    });
    
    const readCSV = (route) => new Promise((resolve, reject) => {
        const resultsProducts = [];

        fs.createReadStream(route)
            .pipe(parse({ delimiter: ",", from_line: 2, cast: true }))
            .on('data', function (data) {
                resultsProducts.push(data)
            })
            .on('end', function () {
                resolve(resultsProducts)
            })
            .on('error', function (err) {
                reject(err);
            });
    });

    const writeToCsv = (values, filepath, columns)  => {
       
        const writableStream = fs.createWriteStream(filepath)
            
        const stringifier = stringify({ header: true, columns: columns })
        
       
        for (const [key, value] of Object.entries(values)) {
            stringifier.write({ [columns[0]]: key, [columns[1]]: value })
        }

        stringifier.pipe(writableStream);
        console.log("Finished writing data")
        
    }

    
    const resultsOrders = await promiseOrders()
    const resultProducts = await readCSV("../master-files/products.csv")
    
    // TASK 1

    // Pasamor un indentificador, recorremos los productos buscándolo y devolvemos el precio unitario
    const productPrice = (identifier) => { 
        let total = 0;
       resultProducts.map((price, index) => {
           if (identifier === index) 
               total = price[2] 
       });
        return total
    }


    // Recorremor Ordenes y las obtenemos por separado devolviendo el total de la venta
    const totalOrderPrice = () => { 
        const totalValue = resultsOrders.map(orderNumber => {
           let value = 0
            orderNumber.slice(2).map(result => { 
                value += productPrice(result)
            })
            
            let sumOrder = value
            value = 0
            return sumOrder
        })
        return totalValue
    }
    
    

    console.log('Writing data Task 1')
    const valueOrder = totalOrderPrice()
    const filepathOrders = "../result-files/order_prices.csv";
    const columnsOrders = ["id", "euros"];
    writeToCsv(valueOrder, filepathOrders, columnsOrders)
}


Trasks()


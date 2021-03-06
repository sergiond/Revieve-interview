const fs = require('fs')
const { parse } = require("csv-parse")
const { stringify } = require("csv-stringify")
const _ = require('lodash')

async function Assignment() {

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

    const writeToCsv = (values, filepath, columns) => {

        const writableStream = fs.createWriteStream(filepath)

        const stringifier = stringify({ header: true, columns: columns })


        for (const [key, value] of Object.entries(values)) {
            stringifier.write({ [columns[0]]: key, [columns[1]]: value })
        }

        stringifier.pipe(writableStream);
        console.log("Finished writing data")

    }

    const writeToCsvMarketing = (values, filepath, columns) => {

        const writableStream = fs.createWriteStream(filepath)

        const stringifier = stringify({ header: true, columns: columns })


        for (const [key, value] of Object.entries(values)) {
            stringifier.write({ [columns[0]]: value.id, [columns[1]]: value.firstname, [columns[2]]: value.lastname, [columns[3]]: value.total_euros })
        }

        stringifier.pipe(writableStream);
        console.log("Finished writing data")

    }


    const resultsOrders = await promiseOrders()
    const resultProducts = await readCSV("../master-files/products.csv")
    const resultCustomers = await readCSV("../master-files/customers.csv")

    // TASK 1

    const firstTrask = () => {
        const productPrice = (identifier) => {
            let total = 0;
            resultProducts.map((price, index) => {
                if (identifier === index)
                    total = price[2]
            });
            return total
        }


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

        return valueOrder

    }

    // TASK 2

    const secondTrask = (resultsOrders) => {

        const buyerID = () => {
            const res = [];
            resultsOrders.map(buyerid => {
                buyerid.slice(2).map(result => {
                    res.push({ id: result, customer_ids: buyerid[1] })
                })

            });

            const productByCostumber = _.uniqWith(res, _.isEqual)

            let response = [];
            productByCostumber.map(order => {
                response[order.id] = [response[order.id], order.customer_ids].join(' ')
            });
            return response
        }

        console.log('Writing data Task 2')
        const buyerOrder = buyerID()
        const filepathBuyerOrders = "../result-files/product_customers.csv"
        const columnsBuyerOrders = ["id", "customer_ids"];
        writeToCsv(buyerOrder, filepathBuyerOrders, columnsBuyerOrders)
    }


    // TRASK 3

    const thirdTrask = () => {
        const totalEuros = () => {
            const res = [];
            valueORderResponse = valueOrder.map(customerorder => {
                return customerorder
            })
            resultsOrders.map((buyerid, index) => {
                buyerid.slice(1, 2).map(result => {
                    res.push({ id: result, customer_order: valueORderResponse[index] })
                })
            })


            const response = res.reduce((res, b) =>
                res.set(b.id, (res.get(b.id) || 0) + Number(b.customer_order)), new Map);


            toObject = (map) => {
                let obj = Object.create(null);
                let i = 1;
                for (let [key, value] of map.entries()) {

                    obj[key] = value;

                }
                return obj;
            }

            const totalOrderPrice = toObject(response)
            const totalOrderPriceObj = resultCustomers.map((totalEur, index) => {
                return { id: totalEur[0], firstname: totalEur[1], lastname: totalEur[2], total_euros: totalOrderPrice[index] || 0 }
            })

            return _.orderBy(totalOrderPriceObj, ['total_euros'], ['desc'])
        }

        console.log('Writing data Task 3')
        const totalEurosResponse = totalEuros()
        const filepathBuyerOrders = "../result-files/customer_ranking.csv"
        const columnsBuyerOrders = ["id", "firstname", "lastname", "total_euros"];
        writeToCsvMarketing(totalEurosResponse, filepathBuyerOrders, columnsBuyerOrders)

    }


    const valueOrder = firstTrask()
    secondTrask(resultsOrders)
    thirdTrask()
}

Assignment()



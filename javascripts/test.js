var asciichart = require("asciichart")
var DataFrame = require("dataframe-js").DataFrame
var redis = require("redis")

client = redis.createClient(6379,"127.0.0.1")

function tickerClient(c) {
}

c = new tickerClient(client);
a = c.get_order_book();

console.log(this.server_status)


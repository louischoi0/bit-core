var asciichart = require("asciichart")
var DataFrame = require("dataframe-js").DataFrame
var redis = require("redis")

var net_client = require('net');

function get_connection() {

  c = net_client.connect({port: 8471,host:'localhost'},function (){
    console.log("connection established")});

  client.on("close",function() { console.log("connection closed")});
  client.on("error",function(err) { console.log(err) });

  client.on('data',function(data) {
    console.log(data)
  });
  
  return c

}

net_client = get_connection()



client = redis.createClient(6379,'127.0.0.1')


client.get("foo", function(err, reply){
  if (err) {
    console.log(err)
  }
  console.log(reply)
})





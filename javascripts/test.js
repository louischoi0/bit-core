var asciichart = require("asciichart")
var DataFrame = require("dataframe-js").DataFrame
var redis = require("redis")

const SERVER_STATUS_KEY = "server"

client = redis.createClient(6379,"127.0.0.1")

function tickerClient(c){
  this.timestamp
  this.c = c
 
  this.server_status = -1
  this.request_sym = -1

  this.nowtsp = function () {
    return new Date().getTime()
  }

  this.get_order_book = function (){
    this.c.get(SERVER_STATUS_KEY, function(e,r) { 

      if (e) {
        console.log(e);
        return;
      }
      
      console.log(r)

      if ( r == 0 ) {
        return;
      }
     
      console.log(this.server_status)

      request_time = this.nowtsp();       
      this.c.set(SERVER_STATUS_KEY,request_time);
    
      
      this.render(request_time);

    });
  }

  this.render = function (tsp) {
    console.log("render");

    this.c.get(tsp, function (e,r) {
      console.log(r);
    })

  }

}

c = new tickerClient(client);
a = c.get_order_book();

console.log(this.server_status)


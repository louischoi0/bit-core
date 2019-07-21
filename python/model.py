import redis 
import numpy as np
from time import sleep
import sys
from bot import serverBot
import time 
import pandas as pd

class redisCon :
    def __init__(self) :
        self.db = redis.StrictRedis(host='localhost', port=6379, db=0,decode_responses=True)

if not __name__ == "__main__" : 
    CONNECTION = redisCon()
   
class eventLambda :
    def __init__(self,f,event_name) :
        self.f = f
        self.event_name = event_name

    def __call__(self,nets,volumes) :
        return self.f(nets,volumes)

class evalCoinTicker :
    def __init__(self,con,coin) :
        self.db = con.db

        self.net_lambda = lambda n,v : (n[0] - n[-1]) / n[-1] * 100
        self.volume_lambda = lambda n,v : (v[0] - v[-1]) / v[-1] * 100

        self.event_lambdas = []
        self.coin = coin

    @staticmethod
    def get_target_key(target,field) :
        return "coinone-" + target + "-" + field

    def getdata(self,target_key,window) :
        v = self.db.lrange(target_key,0,window)
        v = list(map(lambda x : float(x),v))
        return np.array(v) 

    def get_field_data(self,target,field,window) :
        target_key = evalCoinTicker.get_target_key(target,field) 
        return self.getdata(target_key,window)

    def add_event_lambda(self,f,name) :
        self.event_lambdas.append(eventLambda(f,name))

    def routine(self,window) :

        nets = self.get_field_data(self.coin,"last",window)
        volumes = self.get_field_data(self.coin,"volume",window)
            
        _lambda = lambda event_f : ( event_f(nets,volumes) , event_f.event_name, self.coin )

        signals = list(map(_lambda, self.event_lambdas))
        signals = list(filter(lambda res : res[0] < 0 ,signals))

        return signals


class discriptor :

    def __init__(self) :
        pass

    def volume_volatility(self) :
        pass

class maestro :
    def __init__(self,con) :
        self.target_coins = [ "btc","eth","bsv","xrp","eos" ] 
        self.ticker_entry = {}         
        self.redis_connection = con
        self.bot = serverBot()
        self.db = con.db

    def bind_event_to(self,ticker,f,event_name) :
        """
        f is function(ticker) and return lambda n,s :v 
        """
        ticker.add_event_lambda(f(ticker),event_name)

    def init_ticker(self,coin) :
        e = evalCoinTicker(self.redis_connection,coin)
        self.ticker_entry[coin] = e
        return e

    def init_all_tickers(self) :
        return list(map(lambda x : self.init_ticker(x), self.target_coins))
    
    def add_event_net_to_all_ticker(self,thres,window) :
        op = 1 if thres > 0 else -1
        net_event_lambda = lambda _ticker : lambda n,v : (-_ticker.net_lambda(n,v) + thres) * op

        p_or_n = "P" if thres > 0 else "N" 
        p_or_n = "Z" if thres == 0 else p_or_n

        event_name = "Net Event {} {}".format(p_or_n,thres)
        
        self.add_function_to_all_ticker(net_event_lambda,event_name)

    def add_event_volume_to_all_ticker(self,thres,window) :
        op = 1 if thres > 0 else -1
        volume_event_lambda = lambda _ticker : lambda n,v : (-_ticker.volume_lambda(n,v) + thres) * op

        p_or_n = "P" if thres > 0 else "N" 
        p_or_n = "Z" if thres == 0 else p_or_n

        event_name = "Volume Event {} {}".format(p_or_n,thres)
        
        self.add_function_to_all_ticker(volume_event_lambda,event_name)

    def add_function_to_all_ticker(self,f,name) :
        for coin in self.ticker_entry :
            _ticker = self.ticker_entry[coin]
            self.bind_event_to(_ticker,f,name)

    def run(self,window) :

        for coin in self.ticker_entry :
            _ticker = self.ticker_entry[coin]
            r =  _ticker.routine(window)
            self.handle_event_occured(r)

        self.check_streaming()

    def main(self,window) :
        while(True) :
            now = pd.Timestamp.now()
            print(now)
            self.run(window)
            sleep(3)

    @staticmethod
    def unpack_event_name(ename) :
        elems = ename.split(" ")
        field, _, direction,thres = elems
        return field,direction,float(thres)

    def handle_event_tuple(self,event_tuple) :
        overv, event_string, code = event_tuple
        field, dirc, thres = self.unpack_event_name(event_string)
       
        op = 1 if thres > 0 else -1

        dirc = "Arises" if dirc == "P" else "Falls"
        msg = "{} {} {} by {}.".format(code,field,dirc, -thres+(overv*op) )

        self.bot.send_message(msg)

    def handle_event_occured(self,events) :
        return list( self.handle_event_tuple(e) for e in events)

    def check_streaming(self) :

        tcount = len(self.db.lrange("timestamp-series",0,1000000)) - 200 
    
        coins = self.db.lrange("targetcoincode", 0, 1000)

        coinkeys = list("coinone-" + x + "-last" for x in coins)
        ccounts = np.array( list( int(len(self.db.lrange(x,0,1000000))) for x in coinkeys) )
        
        if np.any ( ccounts < tcount ) :
            idx, = np.where ( ccounts < tcount)

            for c in np.array(coins)[idx] :
                msg = "{} data is missing!!".format(c)
                print(msg)
                self.bot.send_message(msg)
                sys.exit(1)

if __name__ == "__main__" :

    redis_connection = redisCon()

    e = evalCoinTicker(redis_connection,"btc")
    e.add_event_lambda(lambda n,s: e.net_lambda(n,s) ,"Net event")

    m = maestro(redis_connection)
        
    m.init_all_tickers()
    m.add_event_net_to_all_ticker(-1,10)
    m.add_event_net_to_all_ticker(0.85,10)
    m.add_event_net_to_all_ticker(0.5,10)

#   for test 
#    m.add_event_net_to_all_ticker(0.01,10)

    m.add_event_volume_to_all_ticker(-10,10)
    m.add_event_volume_to_all_ticker(1.8,10)

    m.main(20) 
    sys.exit(0)



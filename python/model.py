import redis 
import numpy as np
from time import sleep
import sys

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
        self.net_lambda = lambda v : (v[0] - v[-1]) / v[-1] * 100
        self.event_lambdas = []
        self.coin = coin

    @staticmethod
    def get_target_key(target,field) :
        return target + "-" + field

    def getdata(self,target_key,window) :
        v = self.db.lrange(target_key,0,window)
        v = list(map(lambda x : float(x),v))
        return np.array(v) 

    def get_field_data(self,target,field,window) :
        target_key = evalCoinTicker.get_target_key(target,field) 
        return self.getdata(target_key,window)
        
    def task(self,target,field,window,func) :
        v = self.get_field_data(target,field,window)
        return func(v)

    def net(self,window) :
        return self.task(self.coin,"last",window,self.net_lambda)

    def volume_change(self,window) :
        return self.task(self.coin,"volume",window,self.net_lambda)

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
        net_event_lambda = lambda _ticker : lambda n,v : (-_ticker.net_lambda(n) + thres) * op

        p_or_n = "P" if thres > 0 else "N" 
        p_or_n = "Z" if thres == 0 else p_or_n

        event_name = "Net Event {} {}".format(p_or_n,thres)

        for coin in self.ticker_entry :
            _ticker = self.ticker_entry[coin]
            self.bind_event_to(_ticker,net_event_lambda,event_name)

    def run(self,window) :

        for coin in self.ticker_entry :
            _ticker = self.ticker_entry[coin]
            r =  _ticker.routine(window)
            print(r)

if __name__ == "__main__" :
    redis_connection = redisCon()

    e = evalCoinTicker(redis_connection,"btc")
    e.add_event_lambda(lambda n,s: e.net_lambda(n) ,"Net event")

    v = e.net(100)
    print(v)

    m = maestro(redis_connection)
        
    m.init_all_tickers()
    m.add_event_net_to_all_ticker(-0.1,100)
    m.add_event_net_to_all_ticker(0.015,100)

    m.run(100) 
    sys.exit(0)



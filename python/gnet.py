from model import evalCoinTicker, CONNECTION
import sys

COIN = sys.argv[1]
WINDOW = sys.argv[2]

if __name__ == "__main__" :
    e = evalCoinTicker(CONNECTION,COIN)
    v = e.net(WINDOW)
    print(v)



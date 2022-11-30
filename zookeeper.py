import time
import threading
import random

from pathlib import Path
from flask import Flask
import requests

root = Path.cwd().resolve()
brokers = []
leader = None
app = Flask(__name__)

def polling(args):
    global leader

    flag = False
    while True:
        try:
            res = requests.get(f"http://127.0.0.1:{args}/polling", timeout=1)
            flag = True

            if args not in brokers:
                brokers.append(args)
            
            # The first broker to start
            if not leader:
                leader = args

            if leader == args:
                print("---------Leader---------", args)
            else:
                print("---------Follower----------", args)
            
            print(res, threading.get_native_id(), brokers)
            time.sleep(2)
        except Exception as e:
            if flag:
                flag = False
                brokers.remove(args)
                if leader == args and brokers != []:
                    leader = random.choice(brokers)
                    print("------------New Leader----------", leader)
                print(args, "has died$$$$$$$$")
            else:
                print("----Not Started----", args)        


@app.route('/find_leader')
def find_leader():
    return str(leader)

def main():
    broker1 = threading.Thread(target=polling, args=(5000,))
    broker1.daemon = True
    broker2 = threading.Thread(target=polling, args=(5001,))
    broker2.daemon = True
    broker3 = threading.Thread(target=polling, args=(5002,))
    broker3.daemon = True
    broker1.start()
    broker2.start()
    broker3.start()

    app.run(debug=False, use_reloader=False, port=8080)
    

if __name__ == "__main__":
    main()

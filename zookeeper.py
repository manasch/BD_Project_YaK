import time
import threading
import random

from pathlib import Path
from flask import Flask
import requests

root = Path.cwd().resolve()
brokers = [5000, 5001, 5002]
leader = 5001
app = Flask(__name__)

def polling(args):
    global leader
    while True:
        try:
            
            res = requests.get(f"http://127.0.0.1:{args}/polling", timeout=1)

            if leader == args:
                print("---------Leader---------", args)
            else:
                print("---------Follower----------", args)

            if args not in brokers:
                brokers.append(args)
            
            print(res, threading.get_native_id(), brokers)
            time.sleep(2)
        except Exception as e:
            if leader == args:
                brokers.remove(args)
                leader = random.choice(brokers)
                print("------------New Leader----------", leader)
            print(args, "has died$$$$$$$$")

@app.route('/find_leader')
def find_leader():
    return str(leader)

def main():
    broker1 = threading.Thread(target=polling, args=(5000,))
    broker2 = threading.Thread(target=polling, args=(5001,))
    broker3 = threading.Thread(target=polling, args=(5002,))
    broker1.start()
    broker2.start()
    broker3.start()

    app.run(debug=False, use_reloader=False, port=8080)
    

if __name__ == "__main__":
    main()

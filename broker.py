import json
import argparse
from pathlib import Path

import requests
from flask import Flask, request

parser = argparse.ArgumentParser()
parser.add_argument("-p", "--port", help="Set broker port")
parser.add_argument("-i", "--id", help="Set broker ID")
args = parser.parse_args()

app = Flask(__name__)

root = Path.cwd().resolve()
filename = f"Broker_{args.id}"
subscribe_list = root / "subscribe_list.json"
broker_fs = (root / filename).resolve()

@app.route('/')
def main():
    return "Home"

@app.route('/send_topic/<topic>', methods=['POST'])
def send_topic(topic):
    data = json.loads(request.data.decode())
    print(data)
    # requests.post('http://127.0.0.1:6060/', json={topic: "test"})
    return "sent"

@app.route('/create_topic/<topic>')
def create_topic(topic):
    if not broker_fs.exists():
        broker_fs.mkdir()
    
    topic_fs = broker_fs / topic
    if not topic_fs.exists():
        topic_fs.mkdir()
    
    return "created"

@app.route('/subscribe_topic/<topic>', methods=['POST'])
def subscribe_topic(topic):
    port = json.loads(request.data.decode())['port']
    data = {}
    print(port)
    if not subscribe_list.exists():
        subscribe_list.touch()
        data[topic] = [port]
    else:
        with open(subscribe_list) as f:
            try:
                data = json.load(f)
                print(data)
                if topic not in data.keys():
                    data[topic] = [port]
                elif port not in data[topic]:
                    data[topic].append(port)
                else:
                    return "port already registered"
            except Exception as e:
                data[topic] = [port]
    
    with open(subscribe_list, "w") as f:
        json.dump(data, f)
    
    return "subscribed"

@app.route('/unsub_topic/<topic>', methods=['POST'])
def unsub_topic(topic):
    port = json.loads(request.data.decode())['port']
    with open(subscribe_list) as f:
        data = json.load(f)
    
    if port in data[topic]:
        data[topic].remove(port)
        if data[topic] == []:
            del data[topic]
    else:
        return "not subbed to this"
    
    with open(subscribe_list, "w") as f:
        json.dump(data, f)

    return "unsubbed"

@app.route('/polling')
def poll():
    return filename


if __name__ == "__main__":
    app.run(port=args.port, debug=True, use_reloader=True)
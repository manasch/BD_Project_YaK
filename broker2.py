import json
from pathlib import Path
import requests
from flask import Flask, request
app = Flask(__name__)

root = Path.cwd().resolve()
filename = "Broker_2"
broker_fs = (root / filename).resolve()

@app.route('/')
def main():
    return "Home"

@app.route('/send_topic/<topic>', methods=['POST'])
def send_topic(topic):
    data = json.loads(request.data.decode())
    print(data)
    requests.post('http://127.0.0.1:')
    return "sent"

@app.route('/subscribe_topic/<topic>')
def subscribe_topic(topic):
    if not broker_fs.exists():
        broker_fs.mkdir()
    
    topic_fs = broker_fs / topic
    if not topic_fs.exists():
        topic_fs.mkdir()
    
    print("subbed")
    return "subscribed"

@app.route('/polling')
def poll():
    return filename


if __name__ == "__main__":
    app.run()

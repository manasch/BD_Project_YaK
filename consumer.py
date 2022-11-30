import time
import requests
from flask import Flask, request

app = Flask(__name__)
zookeeper_port = 8080
topic_name = "topic_d"

def send_request(url):
    flag = True
    while flag:
        try:
            res = requests.get(url, timeout=1)
            flag = False
        except Exception as e:
            print("Retrying..")
        
    return res.content.decode()

@app.route('/', methods=['POST'])
def receive():
    print(request.data.decode())
    return "Received"

def main():
    leader = send_request(url=f"http://127.0.0.1:{zookeeper_port}/find_leader")
    res = send_request(url=f"http://127.0.0.1:{leader}/create_topic/{topic_name}")
    print(leader, f"http://127.0.0.1:{leader}/create_topic/{topic_name}")
    app.run(port=6060)
    

if __name__ == "__main__":
    main()

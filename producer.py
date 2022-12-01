import time
import requests
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-z", "--port", help="zookeeper port", type=int, required=True)
parser.add_argument("-t", "--topic", help="enter the topic name", required=True)
args = parser.parse_args()

def get_request(url):
    flag = True
    while flag:
        try:
            res = requests.get(url, timeout=1)
            flag = False
        except Exception as e:
            print("Retrying..")
        
    return res.content.decode()

def post_request(url, data):
    flag = True
    while flag:
        try:
            res = requests.post(url, json=data, timeout=1)
            flag = False
        except Exception as e:
            print("Retrying..")
        
    return res.content.decode()

def get_leader():
    return get_request(f"http://127.0.0.1:{args.port}/find_leader")

def main():
    get_request(f"http://127.0.0.1:{get_leader()}/create_topic/{args.topic}")

    while True:
        data_to_send = input()
        json_data = {
            "data": data_to_send
        }
        post_request(f"http://127.0.0.1:{get_leader()}/send_topic/{args.topic}", json_data)

if __name__ == "__main__":
    main()

from pathlib import Path
from flask import Flask
app = Flask(__name__)

root = Path.cwd().resolve()
filename = "Broker_2"
broker_fs = (root / filename).resolve()

@app.route('/')
def main():
    return "Home"

@app.route('/create_topic/<topic>')
def create_topic(topic):
    if not broker_fs.exists():
        broker_fs.mkdir()
    
    topic_fs = broker_fs / topic
    if not topic_fs.exists():
        topic_fs.mkdir()
    
    return "Create"

@app.route('/polling')
def poll():
    return filename


if __name__ == "__main__":
    # print(filename)
    app.run()
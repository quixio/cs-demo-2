import os
import json
import queue
import threading
from collections import deque
from flask import Flask, request, Response, redirect, render_template
from flasgger import Swagger
from waitress import serve
from flask_cors import CORS
from dotenv import load_dotenv

from setup_logging import get_logger
from quixstreams import Application

load_dotenv()

logger = get_logger()

quix_app = Application()
output_topic = quix_app.topic(os.environ["output"])
input_topic = quix_app.topic(os.environ["input"])
producer = quix_app.get_producer()

# --- Shared state for the dashboard ---
data_buffer = deque(maxlen=200)
subscribers = []
subscribers_lock = threading.Lock()


def _broadcast(value: dict):
    with subscribers_lock:
        dead = []
        for q in subscribers:
            try:
                q.put_nowait(value)
            except queue.Full:
                dead.append(q)
        for q in dead:
            subscribers.remove(q)


def consumer_thread():
    with quix_app.get_consumer() as consumer:
        consumer.subscribe([input_topic.name])
        logger.info(f"Consumer subscribed to '{input_topic.name}'")
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            try:
                value = json.loads(msg.value())
                data_buffer.append(value)
                _broadcast(value)
                consumer.store_offsets(message=msg)
            except Exception as e:
                logger.error(f"Error processing message: {e}")


threading.Thread(target=consumer_thread, daemon=True).start()

# --- Flask app ---
app = Flask(__name__)
CORS(app)

app.config['SWAGGER'] = {
    'title': 'HTTP API Source',
    'description': 'Test your HTTP API with this Swagger interface.',
    'uiversion': 3
}
swagger = Swagger(app)


@app.route("/")
def dashboard():
    return render_template("dashboard.html", topic=input_topic.name)


@app.route("/apidocs-redirect")
def redirect_to_swagger():
    return redirect("/apidocs/")


@app.route("/events")
def events():
    """Server-Sent Events stream of magnetometer data."""
    def stream():
        q = queue.Queue(maxsize=500)
        with subscribers_lock:
            subscribers.append(q)

        # Send buffered history first
        for item in list(data_buffer):
            yield f"data: {json.dumps(item)}\n\n"

        try:
            while True:
                try:
                    msg = q.get(timeout=30)
                    yield f"data: {json.dumps(msg)}\n\n"
                except queue.Empty:
                    yield ": keepalive\n\n"
        finally:
            with subscribers_lock:
                if q in subscribers:
                    subscribers.remove(q)

    return Response(stream(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/data/", methods=['POST'])
def post_data_without_key():
    """
    Post data without key
    ---
    parameters:
      - in: body
        name: body
        schema:
          type: object
          properties:
            some_value:
              type: string
    responses:
      200:
        description: Data received successfully
    """
    data = request.json
    logger.debug(f"{data}")
    producer.produce(output_topic.name, json.dumps(data))
    return Response(status=200)


@app.route("/data/<key>", methods=['POST'])
def post_data_with_key(key: str):
    """
    Post data with a key
    ---
    parameters:
      - in: path
        name: key
        type: string
        required: true
      - in: body
        name: body
        schema:
          type: object
          properties:
            some_value:
              type: string
    responses:
      200:
        description: Data received successfully
    """
    data = request.json
    logger.debug(f"{data}")
    producer.produce(output_topic.name, json.dumps(data), key.encode())
    return Response(status=200)


if __name__ == '__main__':
    serve(app, host="0.0.0.0", port=80)

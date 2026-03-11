import os
import datetime
import json
import threading
from collections import deque
from flask import Flask, request, Response, redirect
from flasgger import Swagger
from waitress import serve
import time

from flask_cors import CORS

from setup_logging import get_logger
from quixstreams import Application

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

service_url = os.environ["Quix__Deployment__Network__PublicUrl"]

quix_app = Application()
topic = quix_app.topic(os.environ["output"])
producer = quix_app.get_producer()

logger = get_logger()

# Buffer for accelerometer data (keep last 500 data points)
data_buffer = deque(maxlen=500)
data_lock = threading.Lock()

# dummy line to update commit
def consume_table_data():
    input_topic = quix_app.topic(os.environ["input"])
    with quix_app.get_consumer() as consumer:
        consumer.subscribe([input_topic.name])
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            try:
                value = json.loads(msg.value())
                if all(k in value for k in ["time", "accelerometer-x", "accelerometer-y", "accelerometer-z"]):
                    with data_lock:
                        data_buffer.append(value)
                consumer.store_offsets(message=msg)
            except Exception as e:
                logger.error(f"Error processing message: {e}")


consumer_thread = threading.Thread(target=consume_table_data, daemon=True)
consumer_thread.start()

app = Flask(__name__)

# Enable CORS for all routes and origins by default
CORS(app)

app.config['SWAGGER'] = {
    'title': 'HTTP API Source',
    'description': 'Test your HTTP API with this Swagger interface. Send data and see it arrive in Quix.',
    'uiversion': 3
}

swagger = Swagger(app)

CHART_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Accelerometer Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: #0f1117;
      color: #e0e0e0;
      font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
      display: flex;
      flex-direction: column;
      align-items: center;
      min-height: 100vh;
      padding: 24px;
    }
    h1 {
      font-size: 1.6rem;
      font-weight: 600;
      margin-bottom: 8px;
      color: #ffffff;
      letter-spacing: 0.5px;
    }
    #status {
      font-size: 0.85rem;
      color: #888;
      margin-bottom: 24px;
    }
    #status.live { color: #4caf50; }
    #chart-container {
      width: 100%;
      max-width: 1100px;
      background: #1a1d27;
      border-radius: 12px;
      padding: 24px;
      box-shadow: 0 4px 24px rgba(0,0,0,0.4);
    }
    canvas { width: 100% !important; }
  </style>
</head>
<body>
  <h1>Accelerometer Live Dashboard</h1>
  <div id="status">Connecting...</div>
  <div id="chart-container">
    <canvas id="accelChart"></canvas>
  </div>

  <script>
    const MAX_POINTS = 200;

    const ctx = document.getElementById('accelChart').getContext('2d');
    const chart = new Chart(ctx, {
      type: 'line',
      data: {
        datasets: [
          {
            label: 'X',
            data: [],
            borderColor: '#ef5350',
            backgroundColor: 'rgba(239,83,80,0.1)',
            borderWidth: 2,
            pointRadius: 0,
            tension: 0.3,
          },
          {
            label: 'Y',
            data: [],
            borderColor: '#42a5f5',
            backgroundColor: 'rgba(66,165,245,0.1)',
            borderWidth: 2,
            pointRadius: 0,
            tension: 0.3,
          },
          {
            label: 'Z',
            data: [],
            borderColor: '#66bb6a',
            backgroundColor: 'rgba(102,187,106,0.1)',
            borderWidth: 2,
            pointRadius: 0,
            tension: 0.3,
          },
        ],
      },
      options: {
        animation: false,
        responsive: true,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: {
            labels: { color: '#ccc', font: { size: 13 } },
          },
          tooltip: {
            backgroundColor: '#1e2130',
            titleColor: '#aaa',
            bodyColor: '#eee',
          },
        },
        scales: {
          x: {
            type: 'time',
            time: {
              tooltipFormat: 'HH:mm:ss.SSS',
              displayFormats: { second: 'HH:mm:ss', millisecond: 'HH:mm:ss.SSS' },
            },
            ticks: { color: '#888', maxTicksLimit: 10 },
            grid: { color: 'rgba(255,255,255,0.05)' },
            title: { display: true, text: 'Timestamp', color: '#888' },
          },
          y: {
            ticks: { color: '#888' },
            grid: { color: 'rgba(255,255,255,0.05)' },
            title: { display: true, text: 'Acceleration (g)', color: '#888' },
          },
        },
      },
    });

    let lastTimestamp = null;

    async function fetchData() {
      try {
        const res = await fetch('/chart-data');
        if (!res.ok) throw new Error('Network error');
        const points = await res.json();

        if (!points.length) return;

        // Convert nanoseconds to milliseconds for JS Date
        const newPoints = points.filter(p =>
          lastTimestamp === null || p.time > lastTimestamp
        );

        if (newPoints.length === 0) return;

        lastTimestamp = newPoints[newPoints.length - 1].time;

        newPoints.forEach(p => {
          const ms = p.time / 1_000_000;
          chart.data.datasets[0].data.push({ x: ms, y: p['accelerometer-x'] });
          chart.data.datasets[1].data.push({ x: ms, y: p['accelerometer-y'] });
          chart.data.datasets[2].data.push({ x: ms, y: p['accelerometer-z'] });
        });

        // Trim to MAX_POINTS
        chart.data.datasets.forEach(ds => {
          if (ds.data.length > MAX_POINTS) {
            ds.data.splice(0, ds.data.length - MAX_POINTS);
          }
        });

        chart.update('none');

        document.getElementById('status').textContent =
          `Live  |  Last update: ${new Date().toLocaleTimeString()}  |  ${chart.data.datasets[0].data.length} points`;
        document.getElementById('status').className = 'live';
      } catch (err) {
        document.getElementById('status').textContent = 'Error fetching data: ' + err.message;
        document.getElementById('status').className = '';
      }
    }

    // Poll every 2 seconds
    fetchData();
    setInterval(fetchData, 2000);
  </script>
</body>
</html>
"""


@app.route("/", methods=['GET'])
def dashboard():
    return Response(CHART_HTML, mimetype='text/html')


@app.route("/apidocs-redirect", methods=['GET'])
def redirect_to_swagger():
    return redirect("/apidocs/")


@app.route("/chart-data", methods=['GET'])
def chart_data():
    with data_lock:
        data = list(data_buffer)
    return Response(json.dumps(data), mimetype='application/json')


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

    producer.produce(topic.name, json.dumps(data))

    # Return a normal 200 response; CORS headers are added automatically by Flask-CORS
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

    producer.produce(topic.name, json.dumps(data), key.encode())

    return Response(status=200)


if __name__ == '__main__':
    print("=" * 60)
    print(" " * 20 + "CURL EXAMPLE")
    print("=" * 60)
    print(
        f"""
curl -L -X POST \\
    -H 'Content-Type: application/json' \\
    -d '{{"key": "value"}}' \\
    {service_url}/data
    """
    )
    print("=" * 60)

    serve(app, host="0.0.0.0", port=80)

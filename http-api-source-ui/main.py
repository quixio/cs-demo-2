import os
import json
import time
import queue
import threading
from collections import deque
from flask import Flask, Response, stream_with_context
from flask_cors import CORS
from waitress import serve
from quixstreams import Application
from setup_logging import get_logger
from dotenv import load_dotenv

load_dotenv()

logger = get_logger()

# Shared state
data_lock = threading.Lock()
recent_data = deque(maxlen=200)

sse_clients = []
sse_clients_lock = threading.Lock()

quix_app = Application(
    auto_offset_reset="latest",
    auto_commit_enable=True,
)
input_topic = os.environ["input"]


def consume_messages():
    with quix_app.get_consumer() as consumer:
        consumer.subscribe(topics=[input_topic])
        logger.info(f"Subscribed to topic: {input_topic}")

        while True:
            msg = consumer.poll(0.1)
            if msg is None:
                continue
            elif msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                value = json.loads(msg.value())
                with data_lock:
                    recent_data.append(value)

                event_json = json.dumps(value)
                with sse_clients_lock:
                    for client_q in sse_clients:
                        try:
                            client_q.put_nowait(event_json)
                        except queue.Full:
                            pass  # slow client, skip

                consumer.store_offsets(message=msg)
            except Exception as e:
                logger.error(f"Error processing message: {e}")


consumer_thread = threading.Thread(target=consume_messages, daemon=True)
consumer_thread.start()

app = Flask(__name__)
CORS(app)

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Magnetometer Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      background: #0d1117;
      color: #e6edf3;
      font-family: 'Segoe UI', system-ui, sans-serif;
      min-height: 100vh;
      padding: 24px;
    }

    header {
      display: flex;
      align-items: center;
      gap: 12px;
      margin-bottom: 28px;
    }

    header h1 {
      font-size: 1.5rem;
      font-weight: 600;
      letter-spacing: 0.02em;
    }

    .status-dot {
      width: 10px;
      height: 10px;
      border-radius: 50%;
      background: #3fb950;
      box-shadow: 0 0 6px #3fb950;
      flex-shrink: 0;
    }
    .status-dot.disconnected {
      background: #f85149;
      box-shadow: 0 0 6px #f85149;
    }

    .cards {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 16px;
      margin-bottom: 28px;
    }

    .card {
      background: #161b22;
      border: 1px solid #30363d;
      border-radius: 10px;
      padding: 20px 24px;
    }

    .card .label {
      font-size: 0.75rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: #8b949e;
      margin-bottom: 8px;
    }

    .card .value {
      font-size: 2rem;
      font-weight: 700;
      font-variant-numeric: tabular-nums;
      transition: color 0.3s;
    }

    .card.x .value { color: #58a6ff; }
    .card.y .value { color: #3fb950; }
    .card.z .value { color: #f0883e; }

    .card .unit {
      font-size: 0.8rem;
      color: #8b949e;
      margin-top: 4px;
    }

    .chart-container {
      background: #161b22;
      border: 1px solid #30363d;
      border-radius: 10px;
      padding: 20px 24px;
      margin-bottom: 28px;
    }

    .chart-container h2 {
      font-size: 0.9rem;
      font-weight: 600;
      color: #8b949e;
      margin-bottom: 16px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }

    canvas {
      max-height: 320px;
    }

    .table-container {
      background: #161b22;
      border: 1px solid #30363d;
      border-radius: 10px;
      overflow: hidden;
    }

    .table-container h2 {
      font-size: 0.9rem;
      font-weight: 600;
      color: #8b949e;
      padding: 16px 24px 12px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      border-bottom: 1px solid #21262d;
    }

    .table-scroll {
      max-height: 260px;
      overflow-y: auto;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.85rem;
    }

    th {
      position: sticky;
      top: 0;
      background: #1c2128;
      padding: 10px 24px;
      text-align: right;
      font-weight: 600;
      color: #8b949e;
      border-bottom: 1px solid #21262d;
    }

    th:first-child { text-align: left; }

    td {
      padding: 8px 24px;
      text-align: right;
      border-bottom: 1px solid #21262d;
      font-variant-numeric: tabular-nums;
    }

    td:first-child { text-align: left; color: #8b949e; font-size: 0.8rem; }

    tbody tr:last-child td { border-bottom: none; }

    tbody tr:hover { background: #1c2128; }

    .msg-count {
      font-size: 0.75rem;
      color: #8b949e;
      margin-left: auto;
    }

    @media (max-width: 640px) {
      .cards { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header>
    <div class="status-dot disconnected" id="statusDot"></div>
    <h1>Magnetometer Dashboard</h1>
    <span class="msg-count" id="msgCount">0 messages</span>
  </header>

  <div class="cards">
    <div class="card x">
      <div class="label">X Axis</div>
      <div class="value" id="valX">—</div>
      <div class="unit">µT</div>
    </div>
    <div class="card y">
      <div class="label">Y Axis</div>
      <div class="value" id="valY">—</div>
      <div class="unit">µT</div>
    </div>
    <div class="card z">
      <div class="label">Z Axis</div>
      <div class="value" id="valZ">—</div>
      <div class="unit">µT</div>
    </div>
  </div>

  <div class="chart-container">
    <h2>Live Readings</h2>
    <canvas id="chart"></canvas>
  </div>

  <div class="table-container">
    <h2>Recent Messages</h2>
    <div class="table-scroll">
      <table>
        <thead>
          <tr>
            <th>Time</th>
            <th>X (µT)</th>
            <th>Y (µT)</th>
            <th>Z (µT)</th>
          </tr>
        </thead>
        <tbody id="tableBody"></tbody>
      </table>
    </div>
  </div>

  <script>
    const MAX_POINTS = 60;

    const labels = [];
    const dataX = [], dataY = [], dataZ = [];

    const ctx = document.getElementById('chart').getContext('2d');
    const chart = new Chart(ctx, {
      type: 'line',
      data: {
        labels,
        datasets: [
          { label: 'X', data: dataX, borderColor: '#58a6ff', backgroundColor: 'rgba(88,166,255,0.08)', borderWidth: 2, pointRadius: 0, tension: 0.3 },
          { label: 'Y', data: dataY, borderColor: '#3fb950', backgroundColor: 'rgba(63,185,80,0.08)', borderWidth: 2, pointRadius: 0, tension: 0.3 },
          { label: 'Z', data: dataZ, borderColor: '#f0883e', backgroundColor: 'rgba(240,136,62,0.08)', borderWidth: 2, pointRadius: 0, tension: 0.3 },
        ]
      },
      options: {
        responsive: true,
        animation: false,
        interaction: { mode: 'index', intersect: false },
        scales: {
          x: { ticks: { color: '#8b949e', maxTicksLimit: 8, maxRotation: 0 }, grid: { color: '#21262d' } },
          y: { ticks: { color: '#8b949e' }, grid: { color: '#21262d' } }
        },
        plugins: {
          legend: { labels: { color: '#e6edf3', boxWidth: 14 } }
        }
      }
    });

    let msgCount = 0;
    const statusDot = document.getElementById('statusDot');

    function formatTime(ns) {
      const d = new Date(ns / 1e6);
      return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    }

    function fmt(v) {
      return typeof v === 'number' ? v.toFixed(3) : '—';
    }

    function addPoint(msg) {
      const ts = formatTime(msg['time'] || Date.now() * 1e6);
      const x = msg['magnetometer-x'];
      const y = msg['magnetometer-y'];
      const z = msg['magnetometer-z'];

      // Update cards
      document.getElementById('valX').textContent = fmt(x);
      document.getElementById('valY').textContent = fmt(y);
      document.getElementById('valZ').textContent = fmt(z);

      // Update chart
      labels.push(ts);
      dataX.push(x);
      dataY.push(y);
      dataZ.push(z);
      if (labels.length > MAX_POINTS) {
        labels.shift(); dataX.shift(); dataY.shift(); dataZ.shift();
      }
      chart.update('none');

      // Update table (prepend row)
      const tbody = document.getElementById('tableBody');
      const row = tbody.insertRow(0);
      row.innerHTML = `<td>${ts}</td><td>${fmt(x)}</td><td>${fmt(y)}</td><td>${fmt(z)}</td>`;
      while (tbody.rows.length > 50) tbody.deleteRow(tbody.rows.length - 1);

      msgCount++;
      document.getElementById('msgCount').textContent = `${msgCount.toLocaleString()} messages`;
    }

    function connect() {
      const es = new EventSource('/stream');

      es.addEventListener('open', () => {
        statusDot.classList.remove('disconnected');
      });

      es.addEventListener('message', (e) => {
        const payload = JSON.parse(e.data);
        if (payload.type === 'history') {
          payload.data.forEach(addPoint);
        } else {
          addPoint(payload.data);
        }
      });

      es.addEventListener('error', () => {
        statusDot.classList.add('disconnected');
        es.close();
        setTimeout(connect, 3000);
      });
    }

    connect();
  </script>
</body>
</html>
"""


@app.route("/")
def index():
    return DASHBOARD_HTML


@app.route("/stream")
def stream():
    client_q = queue.Queue(maxsize=100)
    with sse_clients_lock:
        sse_clients.append(client_q)

    @stream_with_context
    def generate():
        try:
            with data_lock:
                history = list(recent_data)
            if history:
                yield f"data: {json.dumps({'type': 'history', 'data': history})}\n\n"

            while True:
                try:
                    msg = client_q.get(timeout=15.0)
                    yield f"data: {json.dumps({'type': 'update', 'data': json.loads(msg)})}\n\n"
                except queue.Empty:
                    yield ": keepalive\n\n"
        finally:
            with sse_clients_lock:
                try:
                    sse_clients.remove(client_q)
                except ValueError:
                    pass

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


if __name__ == "__main__":
    logger.info(f"Starting Magnetometer Dashboard, consuming from topic: {input_topic}")
    serve(app, host="0.0.0.0", port=80)

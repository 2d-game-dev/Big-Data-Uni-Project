# ⌨️ Typing Master – Real-Time Analytics with Kafka & Spark

A modern typing speed test application that demonstrates a complete Big Data streaming pipeline using JavaScript, Flask, Apache Kafka, and Apache Spark.

The project allows users to complete typing tests while their keystroke events are streamed through Kafka and analyzed in real time by Spark Streaming.

---

# 📌 Project Overview

Typing Master is a web-based typing speed test platform that measures:

- Words Per Minute (WPM)
- Typing Accuracy
- Keystroke Statistics
- User Performance Trends

Unlike traditional typing websites, this project streams typing events into Apache Kafka and processes them using Apache Spark Streaming to generate real-time analytics and leaderboard rankings.

The main goal of this project is to demonstrate a complete end-to-end Big Data architecture.

---

# 🏗️ System Architecture

```text
User
 │
 ▼
Website (HTML/CSS/JavaScript)
 │
 ▼
Flask + Socket.IO Bridge
 │
 ▼
Apache Kafka
 │
 ▼
Apache Spark Streaming
 │
 ▼
Analytics Results
 │
 ▼
Flask API
 │
 ▼
Website Dashboard & Leaderboard
```

---

# 🚀 Technologies Used

## Frontend

- HTML5
- CSS3
- JavaScript (Vanilla JS)
- Socket.IO Client

## Backend

- Python
- Flask
- Flask-SocketIO

## Big Data Components

- Apache Kafka
- Apache Spark Structured Streaming

## Communication

- REST API
- WebSockets (Socket.IO)

---

# ✨ Features

## User Authentication

Users can:

- Create accounts
- Sign in
- Sign out

User sessions are stored using Local Storage.

---

## Typing Test

The application:

- Loads random typing passages
- Measures typing speed
- Calculates accuracy
- Tracks keystrokes in real time
- Displays remaining time

---

## Real-Time Kafka Streaming

When Kafka streaming is enabled:

- Every keystroke generates an event
- Events are sent through Socket.IO
- Flask receives the events
- Kafka stores the events in the `typing-events` topic

Example events:

- SESSION_START
- TEST_START
- KEYSTROKE
- TEST_COMPLETE
- TEST_TIMEOUT

---

## Spark Analytics Engine

Apache Spark continuously consumes data from Kafka.

Spark calculates:

- Average WPM
- Average Accuracy
- Total Keystrokes
- Performance Trends
- Global Ranking

Analytics are updated automatically while users are typing.

---

## Live Dashboard

The website displays:

- Current WPM
- Accuracy Percentage
- Time Remaining
- Spark Analytics Card

Spark-generated insights appear directly on the interface.

---

## Global Leaderboard

Spark generates ranking information for all users.

Leaderboard data includes:

- Username
- Average WPM
- Average Accuracy

The leaderboard updates automatically as new data is processed.

---

# 📊 Big Data Workflow

## Step 1 – User Types

A user starts a typing test.

JavaScript records typing activity and creates typing events.

---

## Step 2 – Kafka Producer

The Flask bridge receives events through Socket.IO and sends them to Kafka.

Topic:

```text
typing-events
```

---

## Step 3 – Spark Streaming

Spark continuously listens to Kafka.

Incoming events are analyzed and aggregated in real time.

Metrics include:

- Average typing speed
- Accuracy
- User performance statistics

---

## Step 4 – Analytics Delivery

Spark sends processed results back to Flask through a REST API.

Endpoint:

```text
/api/spark_result
```

---

## Step 5 – Real-Time Dashboard Update

Flask broadcasts analytics to connected clients using Socket.IO.

Users immediately see:

- Updated statistics
- New rankings
- Leaderboard changes

---

# 📁 Project Structure

```text
TypingMaster/
│
├── index.html
├── style.css
├── function.js
│
├── kafka_bridge.py
├── spark_analytics.py
│
├── screenshots/
│   ├── homepage.png
│   ├── typing-test.png
│   ├── analytics.png
│   └── leaderboard.png
│
└── README.md
```

---

# 📈 Spark Analytics Metrics

Spark computes:

| Metric | Description |
|----------|-------------|
| Average WPM | User's average typing speed |
| Average Accuracy | Typing precision |
| Keystroke Count | Total keystrokes processed |
| Global Rank | Relative performance compared to others |
| Performance Trend | Skill classification |

Performance categories:

- 🚀 Fast Typist
- 👍 Moderate
- 📚 Learning

---

# 🔌 API Endpoints

## Get User Statistics

```http
GET /api/stats/<user_id>
```

Returns Spark-generated statistics for a user.

---

## Get Leaderboard

```http
GET /api/leaderboard
```

Returns top performers ranked by typing speed.

---

## Health Check

```http
GET /api/health
```

Returns:

- Kafka connection status
- Active sessions
- Analytics availability

---

# 🎯 Educational Purpose

This project was developed to demonstrate:

- Event Streaming
- Real-Time Data Processing
- Kafka Message Queues
- Spark Structured Streaming
- WebSocket Communication
- Big Data Architecture

It shows how data flows from a web application into a distributed analytics pipeline and back to users in real time.


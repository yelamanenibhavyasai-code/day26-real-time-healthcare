# 🏥 Day 26 — Real-Time Healthcare Monitoring with Apache Spark

## 📌 Project Overview

This project demonstrates a real-time patient vital monitoring pipeline using Scala and Apache Spark Streaming. It processes incoming patient readings, identifies abnormal values using configurable demo thresholds, tracks repeated abnormal readings, and calculates recent patient activity through window operations.

**Disclaimer:** This is an educational simulation using synthetic data. It is not a medical device and must not be used for diagnosis or clinical decisions.

## 🎯 Objectives

- Design a patient vital event schema.
- Process streaming vital readings.
- Generate alerts for abnormal readings.
- Use broadcast variables to distribute thresholds.
- Use accumulators to count abnormal readings in the demo.
- Demonstrate stateful and window-based processing.
- Understand partitions, DAG execution, and fault tolerance.

## 🛠️ Technology Stack

- Scala 2.12.18
- Apache Spark 3.5.3
- Spark Streaming (DStreams)
- SBT 1.12.15
- Java 17
- Ubuntu on WSL

## 🩺 Patient Vital Event Schema

Each incoming event uses this CSV format:

`patientId,timestamp,heartRate,spo2,temperature`

Example:

`P101,10:00:00,120,91,38.5`

| Field | Description |
|---|---|
| patientId | Synthetic patient identifier |
| timestamp | Reading timestamp |
| heartRate | Heart rate in beats per minute |
| spo2 | Oxygen saturation percentage |
| temperature | Temperature in degrees Celsius |

## 🔄 Architecture

1. A TCP socket receives synthetic patient vital events.
2. Spark Streaming processes the input in 10-second micro-batches.
3. Incoming records are parsed and validated.
4. Broadcast thresholds are used to classify readings.
5. Abnormal readings generate console alerts.
6. Stateful processing tracks abnormal readings per patient.
7. A 60-second window calculates recent reading counts.
8. An accumulator tracks the demo's abnormal-reading count.

## ⚙️ Demo Thresholds

The sample application uses these configurable demonstration thresholds:

- Heart rate below 60 or above 100
- SpO₂ below 95%
- Temperature above 38°C

These values are simplified examples, not universal clinical limits.

## 🪟 Streaming Configuration

- **Batch interval:** 10 seconds
- **Window size:** 60 seconds
- **Sliding interval:** 10 seconds
- **Input socket:** `localhost:9999`

## 🚨 Sample Input

```text
P101,10:00:00,120,91,38.5
P101,10:00:10,125,90,38.7
P202,10:00:20,75,98,36.8
P303,10:00:30,110,92,38.2
```

## 📊 Concepts Demonstrated

### Broadcast Variables
Share the threshold configuration with Spark tasks without repeatedly sending the full configuration with each record.

### Accumulators
Count abnormal readings for demonstration and monitoring. Accumulators are not a reliable, exactly-once alert ledger.

### Stateful Processing
Maintain a per-patient count of abnormal readings across micro-batches.

### Window Processing
Calculate the number of readings per patient in the most recent 60-second window, updated every 10 seconds.

### Partitions and DAG
Spark divides distributed datasets into partitions and represents transformations as a directed acyclic graph (DAG) for execution planning.

### Fault Tolerance
Spark can recompute lost partitions using lineage. Streaming recovery requires appropriate checkpointing and recovery configuration; this local demo does not establish production-grade recovery guarantees.

## ▶️ How to Run

### 1. Start the socket server

```bash
nc -lk 9999
```

### 2. Run the Spark application in another terminal

```bash
cd ~/day26-real-time-healthcare
sbt "runMain Day26HealthcareMonitoring"
```

### 3. Send synthetic readings

Enter the sample CSV records into the socket terminal, pressing Enter after each line.

### 4. Inspect the Spark output

Look for abnormal vital alerts, the abnormal-reading counter, repeated-reading messages, and the recent 60-second reading counts.

## ⚠️ Limitations

- Uses synthetic input rather than a hospital device or live clinical system.
- Uses a local TCP socket and local Spark execution.
- The repeated-reading counter is cumulative and does not represent a clinically validated consecutive-reading rule.
- The timestamp is retained as event metadata; the current implementation does not perform event-time watermarking.
- Production deployment would require secure ingestion, access controls, validated clinical rules, monitoring, durable alert storage, and tested recovery procedures.

## 👩‍💻 Author

Yelamaneni Bhavyasai

# ⚡ Smart Meter AI Dashboard

An AI-powered backend system for **transformer monitoring, smart meter analytics, and energy insights** built using FastAPI.

This project provides real-time APIs for:

* 📊 KPI dashboards
* ⚡ Transformer health monitoring
* 🚨 Anomaly detection
* 📈 Forecasting & insights
* 🌍 Geo-based meter visualization

---

## 🚀 Features

* 🔹 Smart Meter KPI Aggregation
* 🔹 Transformer Health Analysis
* 🔹 Energy Consumption Insights
* 🔹 Anomaly Detection using AI/ML
* 🔹 Forecast Precomputation
* 🔹 REST APIs for Dashboard Integration
* 🔹 Scalable FastAPI Architecture

---

## 🏗️ Project Structure

```
tran_dashboard_server/
│
├── main.py                     # FastAPI entry point
├── tabs_router.py              # API routes
├── state.py                    # Global state & DB loading
├── stats_helpers.py            # KPI calculations
│
├── transformer_health.py       # Health analytics
├── transformer_anomalies.py    # Anomaly detection
├── transformer_insights.py     # Insights generation
├── transformer_roi.py          # ROI calculations
│
├── precompute_forecasts.py     # Forecast processing
├── requirements.txt            # Dependencies
├── .gitignore                  # Ignore rules
└── README.md                   # Project documentation
```

---

## ⚙️ Tech Stack

* **Backend:** FastAPI
* **Database:** MongoDB
* **Language:** Python 3.11
* **ML/AI:** Custom models for anomaly detection & forecasting

---

## 🔧 Installation

### 1. Clone the repository

```bash
git clone https://github.com/your-username/tran_dashboard_server.git
cd tran_dashboard_server
```

---

### 2. Create virtual environment

```bash
python -m venv venv
venv\Scripts\activate   # Windows
```

---

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

---

### 4. Configure Environment Variables

Create a `.env` file:

```env
MONGO_URI=mongodb://username:password@host:port/dbname?authSource=admin
```

---

### 5. Run the server

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 6001
```

---

## 📡 API Endpoints

### 🔹 Get Dashboard Tabs

```
GET /smartmeterai-dashboard/api/v1/tabs
```

### 🔹 Smart Meter Summary

```
POST /smartmeterai-dashboard/api/v1/summary
```

### 🔹 Transformer Insights

```
POST /smartmeterai-dashboard/api/v1/inverters
```

### 🔹 Meter Geo Locations

```
POST /smartmeterai-dashboard/api/v1/meter-locations
```

---

## ⚠️ Common Issues

### ❌ MongoDB Authentication Failed

* Check username/password
* Ensure `authSource=admin` is correct
* Verify IP whitelist (if using cloud DB)

---

## 📊 Use Cases

* Smart Grid Monitoring
* Energy Load Forecasting
* Fault Detection in Transformers
* Utility KPI Dashboards
* Power Distribution Analytics

---

## 🚀 Future Enhancements

* 🔹 Real-time streaming (Kafka / WebSockets)
* 🔹 Frontend dashboard (React / Angular)
* 🔹 Advanced ML models (LSTM / Transformers)
* 🔹 Multi-tenant support

---

## 🤝 Contributing

Contributions are welcome!
Feel free to fork the repo and submit a pull request.

---

## 📄 License

This project is licensed under the MIT License.

---



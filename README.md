## DCF77 Meteotime Live Monitor

A real-time DCF77 receiver and Meteotime weather decoder running on a Raspberry Pi.
This project decodes the DCF77 signal, extracts Meteotime weather data, and provides a live JSON status output for web-based visualization.

---

## 📡 Overview

This project implements a complete **DCF77 → Meteotime → Web pipeline**:

* 📶 Receive DCF77 signal via ferrite antenna module
* ⏱ Decode time telegram (DCF77)
* 🌦 Decode Meteotime weather data (3-minute triplets)
* 🔄 Provide real-time status via JSON
* 🌐 Display live data in a browser

---

## 🧠 Features

* Real-time DCF77 signal decoding (bit-level)
* Automatic minute frame detection (59 bits + marker)
* Meteotime triplet assembly and decoding
* Continuous JSON status output (~1 Hz)
* Live signal diagnostics:

  * pulse length
  * bit count
  * synchronization state
* Web-ready output for dashboards or embedded pages

---

## 🏗 System Architecture

```
DCF77 Antenna
      │
      ▼
Raspberry Pi (GPIO)
      │
      ▼
Live Decoder (Python)
      │
      ▼
status.json (updated continuously)
      │
      ▼
Web Server / Browser UI
```

---

## 🔌 Hardware

* Raspberry Pi (tested with Pi Zero 2 W)
* DCF77 receiver module (e.g. ferrite antenna module)
* GPIO connection (signal pin)

### Example wiring

| DCF77 Module    | Raspberry Pi            |
| --------------- | ----------------------- |
| VCC             | 3.3V                    |
| GND             | GND                     |
| DATA            | GPIO (e.g. GPIO17)      |
| PON (if present)| Power Enable (often GND)|

> Note: Signal is typically **inverted**.

---

## ⚙️ Installation

### 1. Install dependencies

```bash
sudo apt update
sudo apt install python3 python3-pip
pip3 install gpiozero
```

---

### 2. Clone repository

```bash
git clone https://github.com/johndcf/dcf77-meteotime-live-monitor.git
cd dcf77-meteotime-live-monitor
```

---

### 3. Run the decoder

```bash
python3 dcf77_meteotime_live.py
```

---

## 📄 JSON Output

The script continuously writes a status file:

```
/tmp/status.json
```

Example structure:

```json
{
  "meta": {
    "started_at": "...",
    "updated_at": "..."
  },
  "dcf77": {
    "time": "2026-04-11 12:34",
    "timezone": "CET"
  },
  "signal": {
    "pulse_ms": 100,
    "bit_count": 23,
    "state": "receiving"
  },
  "meteotime": {
    "last_decode": "Light rain, windy",
    "region": "Central Europe"
  }
}
```

---

## 🌐 Web Integration

You can serve the JSON file via a simple web server:

```bash
cd /tmp
python3 -m http.server 8000
```

Then open in browser:

```
http://<raspberrypi-ip>:8000/status.json
```

---

## 🖥 Example HTML Viewer

A simple web page can poll the JSON and display:

* current DCF77 time
* signal status
* latest Meteotime forecast

This can be embedded into dashboards or websites.

---

## 🔄 Optional: Push to Server

The JSON file can be periodically uploaded to a server:

* via `scp`
* via `rsync`
* via systemd timer

This enables remote monitoring.

---

## 🛠 Systemd Autostart (optional)

Create a service:

```bash
sudo nano /etc/systemd/system/dcf77.service
```

Example:

```ini
[Unit]
Description=DCF77 Meteotime Live Decoder
After=network.target

[Service]
ExecStart=/usr/bin/python3 /home/pi/dcf77_meteotime_live.py
Restart=always

[Install]
WantedBy=multi-user.target
```

Enable:

```bash
sudo systemctl enable dcf77
sudo systemctl start dcf77
```

---

## 📊 Notes on Meteotime

* Meteotime data is transmitted in **3-minute triplets**
* Each triplet must be fully received for valid decoding
* Data includes:

  * weather conditions (day/night)
  * wind (direction + strength)
  * temperature
  * anomalies (extreme weather)

---

## 🔗 Related Project

Offline decoder (algorithm reference):

👉 https://github.com/johndcf/dcf77-meteotime-decoder

---


## 🙌 Acknowledgements

* DCF77 time signal (77.5 kHz, Germany)
* Meteotime weather encoding system
* Reverse engineering community contributions

---

## 📸 Example 

* Very simple live web view
<img width="1425" height="171" alt="image" src="https://github.com/user-attachments/assets/df58ba11-3ecb-420d-b77e-3b39c5d4a902" />


---

## ⚠️ Legal Notice

Meteotime is a commercial service and its data format may be subject to intellectual property rights.

This project is intended for **educational and research purposes only**.
It demonstrates the decoding of publicly received DCF77 signals.

The author is not affiliated with, endorsed by, or connected to Meteotime or any related services.

Any use of this project is at your own responsibility.


## 🚀 Future Ideas

* historical logging
* Grafana integration
* MQTT publishing
---


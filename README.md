# Relaxation Create Method Sensors (`vital-sign-node`)

Edge sensor node for multi-participant vital-sign acquisition. Each module pairs a **BBC micro:bit V2** (Grove Shield v2.0) and a **USB webcam** with an **Ubuntu Raspberry Pi** to capture a synchronized 2-minute window and output four physiological metrics:

1. **EDA / 2min** — Phasic Skin Conductance Response (SCR) peak count over 120 seconds
2. **Heart Rate (BPM)** — Mean beats per minute from ear-clip optical pulse intervals
3. **Respiration Rate (/min)** — Integer breaths per minute via V4L2 frame-difference cycle detection
4. **Average Temperature (°C)** — Mean temperature converted from Grove NTC thermistor ADC readings

## Hardware Pinout (micro:bit V2 + Grove Shield v2.0)

| Grove Port | Sensor | Signal Type |
| :--- | :--- | :--- |
| **P0 / P14** | Grove GSR Sensor | Analog (`0–1023`, 5 Hz) |
| **P1 / P15** | Grove Ear-clip Heart Rate Sensor | Digital pulse (`HIGH` per beat) |
| **P2 / P16** | Grove Temperature Sensor (100k NTC) | Analog (`0–1023`, 5 Hz) |
| **USB (Pi)** | USB Webcam (`/dev/video0`) | V4L2 grayscale motion capture |

## Quick Deployment (Raspberry Pi — Ubuntu OS)

```bash
sudo hostnamectl set-hostname mod-02   # increment per Pi: mod-01, mod-02, ...
git clone [https://github.com/LCNMike-0315/Relaxation_Create_Method_Sensors.git](https://github.com/LCNMike-0315/Relaxation_Create_Method_Sensors.git) ~/vital_module
cd ~/vital_module && bash setup.sh
sudo reboot
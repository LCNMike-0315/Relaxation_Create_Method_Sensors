#!/usr/bin/env bash
set -e

echo "=== Installing Vital-Sign Node Dependencies ==="
sudo apt update
sudo apt install -y python3-opencv python3-serial python3-numpy git

sudo usermod -a -G dialout,video "$USER"
chmod +x module_node.py
mkdir -p data

echo "=== Setup Complete ==="
echo "1. Set a unique hostname per Pi: sudo hostnamectl set-hostname mod-02"
echo "2. Reboot once for serial/camera permissions: sudo reboot"
echo "3. Run: python3 module_node.py"
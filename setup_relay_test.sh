#!/bin/bash
set -e

# Hardcoded Values for Reliability
TOKYO_IP="172.31.44.26"
KEY="/home/ubuntu/.ssh/dongjing.pem"

echo ">>> Preparing Tokyo Installer..."
cat > install_tokyo.sh << EOF
#!/bin/bash
set -e
sudo apt-get update -qq
sudo apt-get install -y dante-server -qq

# Find Interface
IFACE=\$(ip route get 8.8.8.8 | awk '{print \$5}')
echo ">>> Detected Interface: \$IFACE"

# Config Dante
echo ">>> Configuring Dante..."
echo "logoutput: /dev/null" | sudo tee /etc/danted.conf
echo "internal: 172.31.44.26 port = 1080" | sudo tee -a /etc/danted.conf
echo "external: \$IFACE" | sudo tee -a /etc/danted.conf
echo "socksmethod: none" | sudo tee -a /etc/danted.conf
echo "clientmethod: none" | sudo tee -a /etc/danted.conf
echo "user.privileged: root" | sudo tee -a /etc/danted.conf
echo "user.unprivileged: nobody" | sudo tee -a /etc/danted.conf
echo "client pass { from: 0.0.0.0/0 to: 0.0.0.0/0 log: error }" | sudo tee -a /etc/danted.conf
echo "socks pass { from: 0.0.0.0/0 to: 0.0.0.0/0 log: error }" | sudo tee -a /etc/danted.conf

# Kernel Tuning
echo ">>> Tuning Tokyo Kernel..."
echo "net.core.default_qdisc=fq" | sudo tee /etc/sysctl.d/99-bbr.conf
echo "net.ipv4.tcp_congestion_control=bbr" | sudo tee -a /etc/sysctl.d/99-bbr.conf
sudo sysctl -p /etc/sysctl.d/99-bbr.conf

echo ">>> Restarting Dante..."
sudo systemctl restart danted
# sleep 2
# sudo systemctl is-active danted
EOF

echo ">>> Uploading Installer to Tokyo..."
scp -o StrictHostKeyChecking=no -i /home/ubuntu/.ssh/dongjing.pem install_tokyo.sh ubuntu@172.31.44.26:~/

echo ">>> Running Installer on Tokyo..."
ssh -o StrictHostKeyChecking=no -i /home/ubuntu/.ssh/dongjing.pem ubuntu@172.31.44.26 "chmod +x install_tokyo.sh && sudo ./install_tokyo.sh"

echo ">>> Updating Benchmark Target to 172.31.44.26..."
sed -i 's/PROXY_HOST = .*/PROXY_HOST = "172.31.44.26"/' /home/ubuntu/benchmark.py

echo ">>> Running Benchmark (Phase 2: SOCKS5 over Peering)..."
source /home/ubuntu/PolyEdge/venv/bin/activate
python3 -u /home/ubuntu/benchmark.py

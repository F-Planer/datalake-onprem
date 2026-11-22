#!/usr/bin/env bash
# Installs Docker Engine (Apache 2.0) from Docker's official repository.
# Does not install Docker Desktop. Run with: sudo bash scripts/01-install-docker.sh
set -euo pipefail

if [[ $EUID -ne 0 ]]; then echo "Run with sudo."; exit 1; fi
TARGET_USER="${SUDO_USER:?Run through sudo from your own user}"
. /etc/os-release

apt-get update
apt-get install -y ca-certificates curl
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc

cat > /etc/apt/sources.list.d/docker.sources <<SRC
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: ${VERSION_CODENAME}
Components: stable
Signed-By: /etc/apt/keyrings/docker.asc
SRC

apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Allows using docker without sudo (the docker group is equivalent to root; fine for the POC).
usermod -aG docker "$TARGET_USER"
systemctl enable --now docker

docker --version
docker compose version
echo
echo "Done. Log out and back in (or run 'newgrp docker') to use docker without sudo."

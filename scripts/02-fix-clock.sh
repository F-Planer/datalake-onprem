#!/usr/bin/env bash
# Fixes the clock: the network blocks NTS (TCP 4460), the Ubuntu 26.04 default.
# Adds plain NTP servers (UDP 123) to chrony; if that is blocked too, sets the
# time from the Date header of an HTTPS server.
# Run with: sudo bash scripts/02-fix-clock.sh
# Optional: NTP_LOCAL=<ip of the network's NTP server> to use it as the first source.
set -euo pipefail
if [[ $EUID -ne 0 ]]; then echo "Run with sudo."; exit 1; fi

NTP_LOCAL="${NTP_LOCAL:-}"
cat > /etc/chrony/sources.d/datalake-poc.sources <<SRC
# Fallback without NTS (network blocks TCP 4460) — DataLake POC
${NTP_LOCAL:+server $NTP_LOCAL iburst}
pool a.ntp.br iburst maxsources 2
pool pool.ntp.org iburst maxsources 2
SRC

systemctl restart chrony
sleep 10
chronyc makestep >/dev/null || true
sleep 2

if chronyc tracking | grep -q '^Reference ID *: 00000000'; then
  echo "NTP (UDP 123) is blocked too; setting the time from an HTTPS server."
  DATE=$(curl -sI https://www.google.com | awk -F': ' 'tolower($1)=="date"{print $2}' | tr -d '\r')
  if [[ -n "$DATE" ]]; then date -s "$DATE"; else echo "Could not get the time over HTTPS."; exit 1; fi
else
  echo "chrony synchronized:"; chronyc sources | grep '^\^\*' || true
fi
# Writes the correct time to the motherboard clock (RTC, in UTC) so it survives a reboot.
command -v hwclock >/dev/null || apt-get install -y util-linux-extra
hwclock --systohc --utc
echo "RTC: $(hwclock --show)"
date

#!/bin/sh
set -e

# Create virtual serial console
socat -d -d PTY,link=/dev/vtty,raw,echo=0 PTY,link=/dev/console_out,raw,echo=0 &

sleep 1

# Start a shell on the console
socat EXEC:'/bin/sh',pty,stderr,setsid,sigint,sane /dev/console_out,raw,echo=0 &

# Start IPMI simulator
exec  /usr/local/bin/ipmi_sim -n -c /ipmi_sim/lan.conf -f /ipmi_sim/sim.emu
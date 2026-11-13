#!/bin/sh
# A/B (and A/A) load: the same load against each server port given, ROUNDS
# times, rotating which server goes first. CPU: the EaaS process and its run
# processes (utime+stime+cutime+cstime, in ticks of /proc: Linux only, so run
# it in the container). Run from dist/server:
#
#   sh ../../tools/telemetry_overhead/ab.sh "<load.py arguments>" ROUNDS PORT...
#   sh ../../tools/telemetry_overhead/ab.sh "300 4 llm 20" 6 5567 5571 5568   # requests clients pipeline warmup
#
# Give two servers with telemetry off (an A/A pair: the noise floor) and one on.
# summarize.py turns the output into medians, differences and CPU per request.
LOAD=$1; ROUNDS=$2; shift 2
HERE=$(dirname "$0")
# The engine spins a core when its stdin is at end of file; give the load
# clients a stdin that stays open and empty (start the servers the same way:
# `tail -f /dev/null | ./engine ai/eaas.py ...`).
IN=$(mktemp -u); mkfifo "$IN"; exec 3<>"$IN"; rm "$IN"
ERR=$(mktemp)
pid_of() { pgrep -f "^\./engine [^ ]+ --port $1( |\$)" | head -1; }
for port in "$@"; do
  [ -n "$(pid_of "$port")" ] || { echo "no server on port $port" >&2; exit 1; }
done
cpu() { awk '{print $14+$15+$16+$17}' "/proc/$1/stat"; }
# A run process's CPU counts for the server once the server has reaped it
reaped() {
  waited=0
  while [ -n "$(pgrep -P "$1")" ] && [ $waited -lt 30 ]; do sleep 1; waited=$((waited + 1)); done
  [ -z "$(pgrep -P "$1")" ] || echo "warning: server $1 still has run processes after 30 s; their CPU is not counted yet" >&2
}
round=1
while [ $round -le $ROUNDS ]; do
  # rotate the order: round r starts with the r-th server
  ports="$*"; skip=$(( (round - 1) % $# )); i=0; first=""; rest=""
  for p in $ports; do if [ $i -lt $skip ]; then rest="$rest $p"; else first="$first $p"; fi; i=$((i + 1)); done
  for port in $first $rest; do
    pid=$(pid_of "$port")
    reaped "$pid"
    before=$(cpu "$pid")
    out=$(./engine "$HERE/load.py" "$port" $LOAD 2>"$ERR" <&3 | grep p50)
    reaped "$pid"
    after=$(cpu "$pid")
    if [ -n "$out" ]; then
      echo "round $round port $port $out cpu_ticks=$((after - before))"
    else
      echo "round $round port $port FAILED: $(grep -v '^\[' "$ERR" | tail -1)"
    fi
  done
  round=$((round + 1))
done
rm -f "$ERR"

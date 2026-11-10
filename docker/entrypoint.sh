#!/bin/sh

ulimit -n 65535

echo "Running: ${*}"

# Run the command in the background and forward SIGTERM/SIGINT to it.
#
# A shell only runs traps after its foreground child exits, so previously
# `docker stop` was ignored until the 10s timeout and the container was then
# SIGKILLed. That added ~10s to every restart (and every recording gap).
"$@" &
child=$!
trap 'kill -TERM "$child" 2>/dev/null' TERM INT
# The first wait returns when a trapped signal arrives; the second collects the
# child's real exit status
wait "$child"
trap - TERM INT
wait "$child"
exit $?

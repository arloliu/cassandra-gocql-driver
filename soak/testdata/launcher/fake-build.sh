#!/usr/bin/env bash
# fake-build.sh is a build that ignores SIGTERM, for the run-night.sh build-stop fixture.
# The ignored disposition survives exec, so the whole build group ignores the forwarded stop.
trap '' TERM INT
exec sleep 300

bind = "0.0.0.0:8080"
workers = 1
worker_class = "gthread"
# 32 threads: enough to service the TCP backlog quickly while keeping
# _raft_lock contention low (OS context-switch overhead grows ~linearly
# with thread count under a hot mutex on a 2-vCPU CI runner).
threads = 32
backlog = 4096
timeout = 30

# Disable HTTP keep-alive so the server closes connections after each response.
# This makes the server the active TCP closer, putting TIME_WAIT on the server
# side (port 8080) rather than exhausting the test harness's ephemeral ports.
keepalive = 0

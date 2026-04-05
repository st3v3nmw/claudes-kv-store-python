bind = "0.0.0.0:8080"
workers = 1
worker_class = "gthread"
threads = 256    # enough to avoid TCP backlog overflow under concurrent load
backlog = 4096   # OS listen queue — prevents SYN drops that cause ~3s client retransmits
timeout = 30

# Disable HTTP keep-alive so the server closes connections after each response.
# This makes the server the active TCP closer, putting TIME_WAIT on the server
# side (port 8080) rather than exhausting the test harness's ephemeral ports.
keepalive = 0

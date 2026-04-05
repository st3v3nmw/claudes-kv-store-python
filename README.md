# Distributed Key-Value Store Challenge

Build a distributed key-value store from scratch using the Raft consensus algorithm.

## Stages

1. **[http-api](https://clstr.io/kv-store/http-api/)** - Implement a basic key-value store with GET/PUT/DELETE operations over HTTP
2. **[persistence](https://clstr.io/kv-store/persistence/)** - Add durability so data survives clean shutdowns (SIGTERM)
3. **[crash-recovery](https://clstr.io/kv-store/crash-recovery/)** - Ensure consistency after unclean shutdowns (SIGKILL)
4. **[leader-election](https://clstr.io/kv-store/leader-election/)** - Form a cluster and elect a leader using Raft
5. **[log-replication](https://clstr.io/kv-store/log-replication/)** - Replicate operations from leader to followers with strong consistency
6. **[log-compaction](https://clstr.io/kv-store/log-compaction/)** - Prevent unbounded log growth through snapshots and truncation
7. **[membership-changes](https://clstr.io/kv-store/membership-changes/)** - Add/remove nodes one at a time without downtime
8. **[joint-consensus](https://clstr.io/kv-store/joint-consensus/)** - Add/remove multiple nodes simultaneously using joint consensus

## Getting Started

1. Read the requirements for the current stage (linked above)
2. Update the `Dockerfile` if required
3. Run `clstr test` to verify your implementation
4. Run `clstr next` to advance when tests pass

## Resources

- [Challenge Overview](https://clstr.io/kv-store/)
- [How clstr Works](https://clstr.io/how-it-works/)
- [CLI Guide](https://clstr.io/guides/cli/)
- [CI/CD Setup](https://clstr.io/guides/ci-cd/)

Run `clstr --help` to see all available commands.

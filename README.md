# Distributed Key-Value Store Challenge

Build a distributed key-value store from scratch using the Raft consensus algorithm.

## Stages

1. **[http-api](https://clstr.io/kv-store/http-api/)** - Store and Retrieve Data
2. **[persistence](https://clstr.io/kv-store/persistence/)** - Data Survives SIGTERM
3. **[crash-recovery](https://clstr.io/kv-store/crash-recovery/)** - Data Survives SIGKILL
4. **[leader-election](https://clstr.io/kv-store/leader-election/)** - Cluster Elects and Maintains Leader
5. **[log-replication](https://clstr.io/kv-store/log-replication/)** - Data Replicates to All Nodes
6. **[membership-changes](https://clstr.io/kv-store/membership-changes/)** - Add and Remove Nodes Dynamically
7. **[fault-tolerance](https://clstr.io/kv-store/fault-tolerance/)** - Cluster Survives Failures and Partitions
8. **[log-compaction](https://clstr.io/kv-store/log-compaction/)** - System Manages Log Growth

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

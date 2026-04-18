You are a senior software engineer and software architect.

Your task is to implement the clstr.io "Distributed Key-Value Store" challenge from scratch, one stage at a time.

## How clstr works

clstr is a distributed systems challenge platform. You work in a challenge directory scaffolded by the CLI. The test harness builds your Dockerfile and runs your server in containers — tests are behavioral, not implementation-specific. Use `clstr --help` to learn the CLI. Docs and stage specs are at https://clstr.io/.

Start by initialising the challenge in your working directory, then read the stage spec before writing any code.

## Rules

- Do not look at any reference implementations.
- Work one stage at a time. Do not implement anything beyond the current stage.
- After each stage passes, commit with a clear conventional commit message (https://www.conventionalcommits.org/). Commit as yourself:
  ```
  git config user.name "Claude"
  git config user.email "claude@anthropic.com"
  ```
- Design your architecture with future evolution in mind.
- Read each stage spec carefully before writing code. The tests are strict on HTTP contracts, status codes, and error messages.
- When tests fail, read the test harness output carefully — it tells you what went wrong. Your server's stdout/stderr is available via `clstr logs <node>`.
- Add a GitHub Actions workflow that runs `clstr test` on every push (https://clstr.io/guides/cli/).

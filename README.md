# SGit

SGit is a fast, secure, and lightweight Git Smart HTTP Protocol server written in Rust, leveraging the performance and safety of **Axum**, **Tokio**, and **libgit2** (`git2-rs`). It acts as a self-hosted Git HTTP backend, offering complete support for Git Smart Protocol v2, bi-directional streaming, fine-grained access control, automatic maintenance, and robust security constraints out of the box.

A **CodeTease** project.

> SGit is not ready for production yet.

---

## Features

- **High-Performance Smart HTTP Backend**: Exposes the Git Smart Protocol (v2-ready) endpoints (`info/refs`, `git-upload-pack`, and `git-receive-pack`) via asynchronous, bi-directional streaming.
- **Auto-Initialization**: Automatically initializes bare Git repositories on the first `git push`, eliminating the need for manual pre-creation of repositories on the server.
- **Deep Namespace Support**: Out-of-the-box support for arbitrarily nested directories/namespaces (e.g., `git clone http://localhost:3000/org/team/project.git`).
- **Secure by Design**:
  - **Path Traversal Protection**: Multi-layered path sanitization checks block malicious path names containing `..`, leading/trailing slashes, redundant slashes (`//`), backslashes (`\`), or empty names.
  - **HTTP Basic Authentication**: Enforces credential checks on all write/push actions while allowing read actions (such as cloning or fetching) to bypass authentication if desired.
  - **Secure Password Hashing**: Avoids plaintext storage by validating user passwords against SHA-256 hashes defined in a simple TOML configuration file.
- **Self-Healing and Automatic Maintenance**:
  - **HEAD Resolution**: Automatically updates the repository's `HEAD` to point to a valid active branch (e.g., `main` or `master`) if it becomes unborn or invalid after a push.
  - **Asynchronous Garbage Collection**: Spawns non-blocking background tasks to execute `git gc --auto` upon successful pushes, keeping disk usage and packfiles optimized.
  - **Graceful Shutdown**: Listens for system interruption signals to clean up ongoing processes and close client connections safely.
  - **Request Timeouts**: Safely terminates stalled or runaway git operations using configurable request timeouts.

---

## Configuration

SGit is highly configurable using process-wide environment variables:

| Environment Variable | Description | Default Value |
|----------------------|-------------|---------------|
| `SGIT_HOST`          | IP address / Host to bind the server to | `0.0.0.0` |
| `SGIT_PORT`          | Port on which the SGit server will run | `3000` |
| `SGIT_DATA_DIR`      | Base directory where the Git repositories are stored | `/var/lib/sgit` |
| `SGIT_USERS_FILE`    | Path to the TOML configuration file containing user credentials | `users.toml` |
| `SGIT_TIMEOUT`       | Execution timeout in seconds for Git streaming operations | `60` |

---

## User Authentication Setup

To enable HTTP Basic Authentication for write operations, create a `users.toml` file (or specify another location via `SGIT_USERS_FILE`).

If the configuration file is **absent**, SGit operates in public-write mode (authentication checks are bypassed entirely).

### Format of `users.toml`
User passwords must be stored as **SHA-256 hexadecimal hashes** to protect credentials at rest.

```toml
[users]
# Password for alice is "supersecret"
alice = "f75778f7425be4db0369d09af37a6c2b9a83dea0e53e7bd57412e4b060e607f7"

# Password for bob is "pass123"
bob = "9b8769a4a742959a2d0298c36fb70623f2dfacda8436237df08d8dfd5b37374c"
```

To generate a SHA-256 hash for a password, you can run:
```bash
echo -n "your_password" | sha256sum
```

---

## Building and Running

### Prerequisites

- **Rust** (Edition 2024, Rust 1.75+ recommended)
- **Git** (CLI must be available on the host `PATH` for streaming operations)
- OpenSSL development libraries (required by `git2` / `openssl-sys` unless fully vendored)

### 1. Build and Run Locally

```bash
# Clone the repository
git clone https://github.com/codetease/sgit
cd sgit

# Build the project in release mode
cargo build --release

# Run SGit with custom variables
SGIT_PORT=3000 SGIT_USERS_FILE=users.toml cargo run --release
```

### 2. Run with Docker

SGit includes a multi-stage `Dockerfile` for streamlined containerized deployment.

```bash
# Build the Docker image
docker build -t sgit:latest .

# Run the container with a mounted volume for persistent repository storage
docker run -d \
  -p 3000:3000 \
  -v /var/lib/sgit:/var/lib/sgit \
  -e SGIT_DATA_DIR=/var/lib/sgit \
  --name sgit-server \
  sgit:latest
```

Or pull from GHCR:
```bash
docker pull ghcr.io/codetease/sgit:latest
```

---

## Testing

SGit features a robust suite of integration and unit tests covering health checks, security path sanitization, concurrent request safety, and authentication.

To run the test suite:
```bash
cargo test
```

*Note: Since integration tests modify or mock environment variables, they are safely run sequentially using internal test locks to prevent race conditions during execution.*

---

## 📖 Usage Examples

Once SGit is running at `http://localhost:3000`, you can interact with it using standard git commands.

### Push to Create/Update a Repository
Pushing to SGit will automatically create the repository if it does not exist (assuming you are authenticated if a `users.toml` is present).

```bash
# Create a local repository and add files
git init my-awesome-project
cd my-awesome-project
echo "# Awesome Project" > README.md
git add README.md
git commit -m "Initial commit"

# Add SGit as remote (replace 'alice' and 'localhost:3000' with your settings)
git remote add origin http://alice@localhost:3000/org/projects/my-awesome-project.git

# Push changes
git push -u origin master
```

### Clone a Repository
Reading is permitted publicly (bypassing authentication), or managed via custom setups.
```bash
git clone http://localhost:3000/org/projects/my-awesome-project.git
```

---

## License

This project is under the **MIT License**.


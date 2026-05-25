<p align='center'>
    <img alt='Logo' src='https://raw.githubusercontent.com/Marekkon5/onetagger/master/assets/onetagger-logo-github.png'>
</p>
<h1 align='center'>The ultimate cross-platform tagger for DJs</h1>

<h3 align='center'><b>
<a href='https://onetagger.github.io/'>Website</a> | <a href='https://github.com/Marekkon5/onetagger/releases/'>Latest Release</a>
</b></h3>
<br>

<p align='center'>
    <img alt='Version Badge' src='https://img.shields.io/github/v/release/marekkon5/onetagger?label=Latest%20Release'>
    <img alt='Supported OS' src='https://img.shields.io/badge/OS-Windows%2C%20Mac%20OS%2C%20Linux-orange'>
    <img alt='Build Status' src='https://img.shields.io/github/actions/workflow/status/marekkon5/onetagger/build.yml?branch=master'>
</p>

<h3 align='center'><b></b></h3>
<hr>

Cross-platform music tagger.
It can fetch metadata from Beatport, Traxsource, Juno Download, Discogs, Musicbrainz and Spotify.
It is also able to fetch Spotify's Audio Features based on ISRC & exact match. 
There is a manual tag editor and quick tag editor which lets you use keyboard shortcuts. Written in Rust, Vue.js and Quasar.

MP3, AIFF, FLAC, M4A (AAC, ALAC) supported.

*For more info and tutorials check out our [website](https://onetagger.github.io/).*

https://user-images.githubusercontent.com/15169286/193469224-cbf3af71-f6d7-4ecd-bdbf-5a1dca2d99c8.mp4


## Installing

You can download latest binaries from [releases](https://github.com/Marekkon5/onetagger/releases)


## Credits
Bas Curtiz - UI, Idea, Help  
SongRec (Shazam support) - https://github.com/marin-m/SongRec

## Support
You can support this project by donating on [PayPal](https://paypal.me/marekkon5) or [Patreon](https://www.patreon.com/onetagger)

## Compilling

### Linux & Mac
Install dependencies: [rustup](https://rustup.rs), [node](https://nodejs.org/en/download/package-manager/), [pnpm](https://pnpm.io/installation)

**Install remaining dependencies**
```
sudo apt install -y lld autogen libasound2-dev pkg-config make libssl-dev gcc g++ curl wget git libwebkit2gtk-4.1-dev
```

**Compile UI**
```
cd client
pnpm i
pnpm run build
cd ..
```

**Compile**
```
cargo build --release
```
Output will be in: `target/release/onetagger`


### Windows
You need to install dependencies: [rustup](https://rustup.rs), [nodejs](https://nodejs.org/en/download/), [Visual Studio 2019 Build Tools](https://aka.ms/vs/16/release/vs_buildtools.exe), [pnpm](https://pnpm.io/installation)

**Compile UI:**
```
cd client
pnpm i
pnpm run build
cd ..
```

**Compile OneTagger:**
```
cargo build --release
```

Output will be inside `target\release` folder.

## Container background worker mode

This repository includes a **container-first background worker runtime** that wraps the existing `onetagger-cli`.
The objective is to preserve upstream OneTagger behavior while making it usable as an always-on API-driven service.

### Scope and architecture (what this is / what this is not)

**Included in scope**
- Always-on worker process, suitable for Docker/Portainer deployment.
- API endpoint to enqueue tagging jobs.
- Internal FIFO queue with **single-job execution** (one job at a time).
- Reuse of upstream CLI logic (`onetagger-cli autotagger`) as the execution engine.
- Externalized configuration and music volumes.
- Automated GHCR publishing workflow.

**Out of scope**
- No rewrite of desktop UI architecture.
- No migration to a full microservice ecosystem.
- No external broker dependency (Redis/RabbitMQ).

### Functional flow

1. Orchestrator sends `POST /jobs` with a file/folder path.
2. Worker accepts request (`202 Accepted`) and adds it to queue.
3. Worker executes queued jobs sequentially.
4. Worker invokes `onetagger-cli autotagger` internally for each job.
5. Job status visibility is available through `GET /status`.

### API specification

#### `GET /health`
Liveness endpoint.

**Response**
```text
ok
```

#### `GET /status`
Returns current queue state.

**Example response**
```json
{
  "running": "c6f1f1b2-6cbf-4f0a-9e0b-b2fb3557e8f7",
  "queued": [
    "0e5f6d9f-1fe0-45b4-8aa5-3fbd478cad69"
  ]
}
```

#### `POST /jobs`
Enqueue a new autotagger job.

**Request body**
```json
{
  "file": "/music",
  "config": "/config/autotagger.json",
  "extra_args": ["--overwrite", "--threads", "4"]
}
```

**Fields**
- `file` (required): input path passed to `onetagger-cli autotagger --path`.
- `config` (optional): config path passed to `--config`.
  - Default: `/config/autotagger.json`.
- `extra_args` (optional): additional CLI flags appended as-is.

**Accepted response (202)**
```json
{
  "id": "0e5f6d9f-1fe0-45b4-8aa5-3fbd478cad69",
  "queue_position": 2
}
```

### Installation and deployment

#### Option A - Pull prebuilt image (recommended)

```bash
docker pull ghcr.io/<owner>/onetagger-worker:latest
```

#### Option B - Build locally

```bash
docker build -f Dockerfile.worker -t onetagger-worker:local .
```

### Runtime prerequisites

- A mounted music library (example: `/music`).
- A mounted configuration directory (example: `/config`).
- A valid OneTagger autotagger configuration JSON available at `/config/autotagger.json` (unless `config` is sent explicitly per job).

### Run with Docker

```bash
docker run -d --name onetagger-worker   -p 8080:8080   -v $(pwd)/config:/config   -v /path/to/your/music:/music   -e RUST_LOG=info   ghcr.io/<owner>/onetagger-worker:latest
```

### Run with Docker Compose / Portainer

Use `docker-compose.worker.yml` as stack template:
- set your published image tag,
- map `/music` to your host music folder,
- map `/config` to persistent host storage.

This enables Portainer to auto-pull image updates without local builds.

### Worker configuration (environment variables)

- `ONETAGGER_WORKER_BIND` (default: `0.0.0.0:8080`)
- `ONETAGGER_CLI_BIN` (default: `/usr/local/bin/onetagger-cli` inside image)
- `ONETAGGER_CONFIG_DIR` (default: `/config`)
- `RUST_LOG` (recommended: `info` or `debug`)

### Troubleshooting and observability

- Use `docker logs -f onetagger-worker` for runtime logs.
- Startup logs show bind address, CLI binary path and config directory.
- Each request logs: job id, queue position, path and custom config usage.
- Each execution logs: resolved config path, extra args and CLI invocation lifecycle.
- Failures include CLI exit code plus stdout/stderr to speed up root-cause analysis.

### Automated GHCR publishing (GitHub CI/CD)

Workflow: `.github/workflows/docker-worker.yml`

**Triggers**
- Push on `master` for worker-related files.
- Tag pushes matching `v*`.
- Manual run (`workflow_dispatch`).

**Output**
- Image: `ghcr.io/<owner>/onetagger-worker`
- Architectures: `linux/amd64`, `linux/arm64`
- Tags: branch/tag/sha + `latest` on default branch


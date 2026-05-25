# Snatcharr

Automation bridge between Whisparr, Prowlarr, SABnzbd and qBittorrent.

Pulls monitored performers from Whisparr, searches Prowlarr for missing scenes, queues them in SABnzbd (usenet preferred) or qBittorrent (torrent fallback), then auto-imports completed downloads back into Whisparr — unattended.

## How it works

1. Reads monitored performers directly from the Whisparr SQLite database
2. On a configurable schedule, searches Prowlarr for any missing scenes
3. Sends NZBs to SABnzbd first; falls back to torrents via qBittorrent if no NZB exists
4. Polls SABnzbd/qBittorrent every 60 seconds for completed downloads
5. Fires a Whisparr ManualImport command when a download finishes
6. Rescans the movie in Whisparr 3 minutes later to confirm the file landed

## Deploy

```yaml
services:
  snatcharr:
    image: ghcr.io/lordmaa/snatcharr:latest
    container_name: snatcharr
    restart: unless-stopped
    ports:
      - "6060:6060"
    environment:
      - WHISPARR_DB=/whisparr-config/whisparr3.db
      - SNATCHARR_DATA=/data
    volumes:
      - /portainer/files/appdata/config/snatcharr:/data
      - /portainer/files/appdata/config/whisparrv3:/whisparr-config:ro
      - /mnt/nas/downloads:/mnt/nas/downloads

volumes:
  snatcharr-data:
```

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `WHISPARR_DB` | `/portainer/files/appdata/config/whisparrv3/whisparr3.db` | Path to the Whisparr SQLite database |
| `SNATCHARR_DATA` | app directory | Directory for `state.json` and `config.json` |

## Volume mounts

| Mount | Purpose |
|---|---|
| `/whisparr-config` | Whisparr config dir (read-only) — contains the database |
| `/portainer/files/appdata/config/snatcharr` → `/data` | Persistent state and settings |
| `/mnt/nas/downloads` | Must match the download path SABnzbd/qBittorrent write to |

## Settings

All service URLs and API keys are configured via the Settings page at `http://<host>:6060/settings`. Saved to `config.json` in the data volume — no env vars needed for credentials.

## Ports

| Port | UI |
|---|---|
| 6060 | Web interface |

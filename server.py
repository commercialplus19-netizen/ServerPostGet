#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
  DnK V4 Relay Server — Relais Bidirectionnel (Double Tunnel PUSH/PULL)
=============================================================================
  Rôle : Tunnel double sens étanche en mémoire :
         1. Canal Descendant (Downstream : Messagerie/APK -> Archive)
            - POST /push ou POST /push?target=archive
            - GET /pull ou GET /pull?target=archive
         2. Canal Ascendant (Upstream : Archive -> Messagerie/APK)
            - POST /push?target=upstream (ou POST /push_upstream)
            - GET /pull?target=upstream (ou GET /pull_upstream)
  Technologies : FastAPI, Uvicorn (sans base de données, 100% mémoire avec TTL)

  Installation :
      pip install fastapi uvicorn
  Lancement :
      uvicorn v4_relay:app --host 0.0.0.0 --port 8080

  Variables d'environnement :
      MAX_QUEUE_SIZE : taille max de la file par canal (défaut 3000)
      TTL_SECONDS    : durée de vie d'un paquet en file (défaut 300)
=============================================================================
"""

import os
import time
import json
from collections import deque
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
MAX_QUEUE_SIZE = int(os.environ.get("MAX_QUEUE_SIZE", "3000"))
TTL_SECONDS = int(os.environ.get("TTL_SECONDS", "300"))

app = FastAPI(
    title="DnK V4 Bi-directional Relay Server",
    version="2.0.0",
    description="Relais bidirectionnel à double tunnel étanche entre le serveur principal et l'archive"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Files d'attente étanches en mémoire (Double Canal)
# -----------------------------------------------------------------------------
queue_downstream = deque()  # Messagerie/APK -> Archive
queue_upstream = deque()    # Archive -> Messagerie/APK

# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------
@app.get("/")
@app.get("/health")
async def health():
    return {
        "status": "online",
        "service": "dnk-v4-bidirectional-relay",
        "version": "2.0.0",
        "downstream_queue_size": len(queue_downstream),
        "upstream_queue_size": len(queue_upstream),
        "max_queue_size": MAX_QUEUE_SIZE,
        "ttl_seconds": TTL_SECONDS,
        "timestamp": int(time.time() * 1000)
    }

@app.post("/push")
async def push(request: Request, target: str = Query("archive")):
    try:
        data = await request.json()
        if not isinstance(data, dict):
            raise HTTPException(status_code=400, detail="Invalid JSON payload")

        data["_received_at"] = int(time.time())
        target_clean = (target or "").lower().strip()

        if target_clean in ("upstream", "server", "apk", "main"):
            selected_queue = queue_upstream
            channel_name = "upstream"
        else:
            selected_queue = queue_downstream
            channel_name = "downstream"

        selected_queue.append(data)

        while len(selected_queue) > MAX_QUEUE_SIZE:
            selected_queue.popleft()

        return {
            "success": True,
            "channel": channel_name,
            "queued": len(selected_queue),
            "message": f"Paquet mis en file d'attente ({channel_name})"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/push_upstream")
async def push_upstream(request: Request):
    return await push(request, target="upstream")

@app.get("/pull")
async def pull(limit: int = 100, target: str = Query("archive")):
    now = int(time.time())
    packets = []
    count = 0
    target_clean = (target or "").lower().strip()

    if target_clean in ("upstream", "server", "apk", "main"):
        selected_queue = queue_upstream
        channel_name = "upstream"
    else:
        selected_queue = queue_downstream
        channel_name = "downstream"

    while selected_queue and count < limit:
        packet = selected_queue[0]

        if now - packet.get("_received_at", now) > TTL_SECONDS:
            selected_queue.popleft()
            continue

        packets.append(selected_queue.popleft())
        count += 1

    return {
        "success": True,
        "channel": channel_name,
        "count": len(packets),
        "packets": packets
    }

@app.get("/pull_upstream")
async def pull_upstream(limit: int = 100):
    return await pull(limit=limit, target="upstream")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)

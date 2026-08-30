#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
  DnK V4 Relay Server — Relais PUSH/PULL entre V3 et Archive locale
=============================================================================
  Rôle : Recevoir les paquets du V3 (POST /push) et les mettre à disposition
         de l'archive locale (GET /pull). Fonctionne en mémoire, sans base.
  Technologies : FastAPI, Uvicorn (identique au V3)

  Lancement :
      uvicorn v4:app --host 0.0.0.0 --port 8080

  Variables d'environnement :
      MAX_QUEUE_SIZE : taille max de la file (défaut 2000)
      TTL_SECONDS   : durée de vie d'un paquet en file (défaut 300)
=============================================================================
"""

import os
import time
import json
from collections import deque
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
MAX_QUEUE_SIZE = int(os.environ.get("MAX_QUEUE_SIZE", "2000"))
TTL_SECONDS = int(os.environ.get("TTL_SECONDS", "300"))

app = FastAPI(
    title="DnK V4 Relay Server",
    version="1.0.0",
    description="Relais PUSH/PULL entre V3 et Archive locale"
)

# CORS (comme le V3)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# File d'attente en mémoire
# -----------------------------------------------------------------------------
queue = deque()
queue_lock = None  # Pas besoin de lock pour une simple démo, mais on peut ajouter plus tard

# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------
@app.get("/")
@app.get("/health")
async def health():
    """État du serveur"""
    return {
        "status": "online",
        "service": "dnk-v4-relay",
        "version": "1.0.0",
        "queue_size": len(queue),
        "max_queue_size": MAX_QUEUE_SIZE,
        "ttl_seconds": TTL_SECONDS,
        "timestamp": int(time.time() * 1000)
    }

@app.post("/push")
async def push(request: Request):
    """
    Reçoit un paquet du V3 et le met en file d'attente.
    """
    try:
        data = await request.json()
        if not isinstance(data, dict):
            raise HTTPException(status_code=400, detail="Invalid JSON payload")

        # Ajouter un timestamp pour la gestion TTL
        data["_received_at"] = int(time.time())

        # Ajouter à la file
        queue.append(data)

        # Limiter la taille de la file
        while len(queue) > MAX_QUEUE_SIZE:
            queue.popleft()

        return {
            "success": True,
            "queued": len(queue),
            "message": "Paquet mis en file d'attente"
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/pull")
async def pull(limit: int = 100):
    """
    L'archive locale récupère les paquets en attente.
    Les paquets sont retirés de la file après avoir été lus.
    """
    now = int(time.time())
    packets = []
    count = 0

    # On retire les paquets de la file jusqu'à atteindre la limite
    while queue and count < limit:
        packet = queue[0]

        # Si le paquet est trop vieux, on le jette
        if now - packet.get("_received_at", now) > TTL_SECONDS:
            queue.popleft()
            continue

        # Sinon, on le sort de la file et on l'ajoute à la réponse
        packets.append(queue.popleft())
        count += 1

    return {
        "success": True,
        "count": len(packets),
        "packets": packets
    }

# -----------------------------------------------------------------------------
# Point d'entrée (si lancé directement)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)

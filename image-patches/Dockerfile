FROM ghcr.io/openclaw/openclaw@sha256:a4dc213ec5fac0e64194bc140691a825551a6f50b7e22a20df836a9e40294427

COPY context-timing.patch.mjs /tmp/context-timing.patch.mjs
RUN node /tmp/context-timing.patch.mjs

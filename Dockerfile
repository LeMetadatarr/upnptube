FROM denoland/deno:bin-2.5.6 AS deno

FROM python:3.12-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -r /var/lib/apt/lists/*
COPY --from=deno /deno /usr/local/bin/deno
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN useradd --create-home --uid 1000 upnptube && install -d -o upnptube /opt/venv /data
USER upnptube
ENV VIRTUAL_ENV=/opt/venv PATH=/opt/venv/bin:$PATH UV_NO_CACHE=1
COPY --chown=upnptube . /src
RUN uv venv /opt/venv \
    && uv pip install --prerelease=allow /src bgutil-ytdlp-pot-provider
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh

VOLUME /data
HEALTHCHECK --interval=60s --timeout=5s \
    CMD python -c "import os, urllib.request; urllib.request.urlopen(f'http://127.0.0.1:{os.environ.get(\"HTTP_PORT\", 8100)}/api/renderers', timeout=4)"
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

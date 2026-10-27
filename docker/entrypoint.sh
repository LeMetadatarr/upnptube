#!/bin/sh
# YouTube changes break old yt-dlp releases within weeks, so start with the latest one.
if [ "${YTDLP_UPDATE:-true}" = "true" ]; then
    uv pip install --quiet --prerelease=allow --upgrade "yt-dlp[default]" bgutil-ytdlp-pot-provider \
        || echo "entrypoint: cannot update yt-dlp; using the installed version" >&2
fi
exec upnptube --state-dir /data --dial-port "${DIAL_PORT:-3100}" --http-port "${HTTP_PORT:-8100}" \
    --name "${SCREEN_NAME:-🔊 {name\}}" "$@"

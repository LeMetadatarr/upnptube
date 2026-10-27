# upnptube

Cast from the YouTube and YouTube Music apps to DLNA/UPnP renderers, such as Wi-Fi speakers,
amplifiers and smart TVs.

Run upnptube on a computer in your local network. It finds every UPnP media renderer, and each
renderer then shows in the cast menu of the YouTube app on Android, iPhone and the desktop
website. From the app you can play, pause, seek, skip, queue videos and change the volume.
When the queue ends, autoplay continues with the video that YouTube picks.

upnptube plays audio only.

## Run it with Docker Compose

    cp .env.example .env
    docker compose up -d --build

The container uses the host network, because phones find the renderers by SSDP multicast.
It keeps the screen ids in the `upnptube-data` volume, so a phone that paired once finds
the renderer again after a restart.

The settings in `.env`:

| Setting | Default | Use |
| --- | --- | --- |
| `DIAL_PORT` | 3100 | The DIAL port of the first renderer; each next renderer uses the next port |
| `HTTP_PORT` | 8100 | The audio streams and the control API |
| `SCREEN_NAME` | `🔊 {name}` | How the YouTube app shows a renderer; `{name}` is the renderer's own name |
| `YTDLP_UPDATE` | true | Update yt-dlp each time the container starts |

The image contains the [bgutil PO token plugin](https://github.com/Brainicism/bgutil-ytdlp-pot-provider)
for yt-dlp. When a bgutil provider listens on `127.0.0.1:4416` of the host, yt-dlp uses it,
and YouTube refuses fewer downloads.

## Run it without Docker

upnptube needs Python 3.11 or later, ffmpeg, and a JavaScript runtime for yt-dlp, such as deno.

    uv tool install --prerelease=allow /path/to/upnptube
    upnptube

Add `--verbose` to log the YouTube and UPnP messages, and `--help` for every option. Give
extra yt-dlp options with `--ytdlp-arg`, for example `--ytdlp-arg=--cookies=/data/cookies.txt`.

## Ports

The computer must be on the same network as the phones and the renderers.

| Port | Protocol | Use |
| --- | --- | --- |
| 1900 | UDP | SSDP: find renderers, and let the YouTube app find upnptube |
| 3100, 3101, … | TCP | One DIAL server for each renderer |
| 8100 | TCP | The audio streams that the renderers fetch, and the control API |

## Control API

castctl and other clients use this API on the HTTP
port. A request names a renderer with `renderer`: part of its name, its UDN or its location
URL. Without a name, the request goes to the only renderer.

| Request | Body or query | Result |
| --- | --- | --- |
| `GET /api/renderers` | | The renderers that are ready |
| `GET /api/status` | `?renderer=` | State, title, artist, video id, position, duration, volume, whether next and previous can play, connected senders |
| `GET /api/pairing-code` | `?renderer=` | A TV code to link the YouTube app, as on a smart TV |
| `POST /api/play` | `{"url": …}` | Play a URL. A YouTube link goes into the queue, with autoplay after it |
| `POST /api/pause`, `resume`, `stop`, `next`, `previous` | | Control the player |
| `POST /api/volume` | `{"volume": 0–100}` | Set the volume |
| `POST /api/seek` | `{"position": seconds}` | Play the track from a position |

The phone and the API drive the same player, so the app shows what the API plays.

## Desktop media controls and KDE Connect

`upnptube-mpris` shows each renderer as an MPRIS player on the D-Bus session bus. The media
controls of the desktop then show what a phone casts, and KDE Connect lets a paired phone
control it. The bridge reads the control API, so it runs in the desktop session, apart from
the service.

Install it on the computer with the desktop session, and start it as a user service:

    uv tool install --prerelease=allow '/path/to/upnptube[mpris]'
    cp /path/to/upnptube/contrib/upnptube-mpris.service ~/.config/systemd/user/
    systemctl --user enable --now upnptube-mpris

For a service on another host, add `--server http://HOST:8100` to `ExecStart`. Each player
has the bus name `org.mpris.MediaPlayer2.upnptube.<renderer name>`, for example
`org.mpris.MediaPlayer2.upnptube.Living_Room_Stereo`.

## How it works

For each renderer, upnptube starts a DIAL server that offers the "YouTube" application. It
also binds one screen session for YouTube and one for YouTube Music to YouTube's Lounge
servers, and keeps them bound. A phone finds the DIAL server and sends it a pairing code, or
a user types the TV code; the phone then controls the screen through the Lounge servers.

To play a video, yt-dlp downloads the audio, and ffmpeg passes the AAC to the renderer as an
ADTS stream, without a re-encode. Audio in other codecs is encoded to MP3. yt-dlp does the
download because YouTube refuses its stream URLs to most other clients. A seek starts the
stream again at the new position.

upnptube polls the renderer each second. When a track ends, the next video plays: the next one
in the queue, or the autoplay video after the last one. When a stream breaks before the end of
its track, upnptube loads it again at the same position, up to twice.

When the last phone leaves, the queue stays if a phone only lost its connection, and clears if
every phone disconnected on purpose.

The Lounge protocol follows [yt-cast-receiver](https://github.com/patrickkfkan/yt-cast-receiver) 2.x.
Discovery follows the UPnP Device Architecture, cast launch follows the DIAL specification, and
renderer control uses the AVTransport and RenderingControl services.

## Test

    uv venv
    uv pip install --prerelease=allow -e '.[test]'
    .venv/bin/pytest tests/test_units.py tests/test_mpris.py

`tests/test_mpris.py` starts its own `dbus-daemon`, so it never touches the players of the desktop.

`tests/e2e_live.py` runs a full cast on 127.0.0.1, so a firewall does not block it. A fake
renderer answers the UPnP calls and reads the stream. A fake phone pairs with a TV code and
controls the screen through YouTube's Lounge servers. It needs internet access:

    .venv/bin/python tests/e2e_live.py [video_id]

## Limitations

- The renderer must play AAC in ADTS, or MP3.
- A seek or a resume after a long pause downloads the audio again up to that position.

## Licence

Apache-2.0. See `LICENSE` and `NOTICE`.

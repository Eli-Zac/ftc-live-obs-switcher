"""
FTC Live Scene Switcher for OBS Studio

Switches the OBS program scene when things happen in FTC Live (FIRST Tech Challenge
scoring): a match comes up on screen, starts, ends, results are posted, and so on.
Rules can be tied to a field, so "match on screen, field 1" can switch to a Field 1
scene and field 2 to another.

It listens to the same feed FTC Live's audience display uses:

    ws://HOST/stream/display/command/?code=EVENT

That feed needs no login, and this only listens: it sends nothing but keep-alive and
clock-sync messages. The periods inside a match (auto end, driver-controlled, endgame,
the buzzer) are not sent as messages; like FTC Live's own display, they are worked
out from START_MATCH's server timestamp and the event's MATCH_TIMING.

Install: OBS > Tools > Scripts > Python Settings (point it at a 64-bit Python 3),
then Scripts > + and pick this file.
"""

import base64
import json
import os
import queue
import re
import socket
import struct
import threading
import time
import urllib.parse
import urllib.request

import obspython as obs

# ---------------------------------------------------------------------------
# Triggers

TRIGGERS = [
    ("preview",     "Match preview"),
    ("show",        "Match on screen"),
    ("countdown",   "Countdown (START pressed)"),
    ("start",       "Match start (auto)"),
    ("autoEnd",     "Auto ends"),
    ("controllers", "Pick up controllers"),
    ("teleop321",   "3-2-1 to driver-controlled"),
    ("teleop",      "Driver-controlled"),
    ("midmatch",    "Mid-match warning"),
    ("endgame",     "Endgame"),
    ("end",         "Match end (buzzer)"),
    ("abort",       "Match aborted"),
    ("posted",      "Results posted (instant)"),
    ("results",     "Results shown - any"),
    ("redWins",     "Results shown - red wins"),
    ("blueWins",    "Results shown - blue wins"),
    ("tie",         "Results shown - tie"),
    ("award",       "Award presented"),
    ("selection",   "Alliance selection"),
    ("message",     "Message posted"),
    ("videoScreen", "Video screen shown"),
    ("video",       "Video starts playing"),
    ("videoEnd",    "Video finished"),
]

# display commands this handles, or deliberately ignores (as seen on FTC Live v8.0.3)
KNOWN_COMMANDS = {"MATCH_TIMING", "SHOW_PREVIEW", "SHOW_MATCH", "START_MATCH", "ABORT_MATCH", "CANCEL_MATCH",
                  "SHOW_RESULTS", "SHOW_AWARD", "SHOW_SELECTION", "SHOW_MESSAGE",
                  "SHOW_SETUP", "SCORE_UPDATE", "TIMEOUT_UPDATE", "PIT_UPDATE", "PLAY_VIDEO", "SHOW_VIDEO",
                  "SHOW_KEY", "SHOW_RANKINGS", "SHOW_INSPECTION_STATUS", "SHOW_BLANK", "SHOW_SPONSORS",
                  "SHOW_BRACKET", "SHOW_ROTATING_BRACKET", "SHOW_ADVANCEMENT", "SHOW_MEET_RANKINGS",
                  "SHOW_ONLINE_RESULTS", "SHOW_ROUND_RESULTS", "SHOW_SAFETY_SECURITY", "SHOW_SLIDESHOW",
                  "SHOW_STATUS", "SHOW_TIMEOUT", "SHOW_WIFI"}
TRIGGER_LABELS = dict(TRIGGERS)

MAX_RULES = 20
MAX_FIELDS = 6

RECONNECT_MIN_S = 1.0
RECONNECT_MAX_S = 10.0
PING_S = 10.0
SILENCE_S = 25.0
LATE_MS = 1500        # a period that began this recently still fires
REVEAL_MS = 4000      # results video length + fade, as measured for the DMX controller
VIDEO_START_MS = 1000 # FTC Live's display starts a PLAY_VIDEO video 1 s after the command's ts...
VIDEO_AFTER_MS = 1000 # ...and goes blank 1 s after the video ends

# display commands that only update the screen that's up; anything else replaces it (so a playing
# video is gone and won't "finish")
UPDATE_ONLY = {"MATCH_TIMING", "SCORE_UPDATE", "TIMEOUT_UPDATE", "PIT_UPDATE", "SET_CONFIG"}

# FTC Live's own fallback when no MATCH_TIMING has arrived
DEFAULT_TIMING = {"countdown": 3000, "auto": 30000, "transition": 8000, "teleop": 120000,
                  "midmatchWarning": 60000, "endgameWarning": 20000}

_ISO = re.compile(r"^(-)?P(?:(\d+(?:\.\d+)?)D)?(?:T(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(-?\d+(?:\.\d+)?)S)?)?$")


def duration_ms(v):
    """"PT2M", "PT30S" (Java Duration) or a number -> ms; None if unreadable."""
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return int(v)
    s = str(v or "").strip()
    m = _ISO.match(s)
    if not m or s in ("P", "PT"):
        return None
    neg, d, h, mi, sec = m.groups()
    ms = round((float(d or 0) * 86400 + float(h or 0) * 3600 + float(mi or 0) * 60 + float(sec or 0)) * 1000)
    return -ms if neg else ms


def read_timing(raw):
    t = dict(DEFAULT_TIMING)
    if isinstance(raw, dict):
        for k in DEFAULT_TIMING:
            v = duration_ms(raw.get(k))
            if v is not None and v >= 0:
                t[k] = v
    return t


def phases(t):
    """Timed triggers, as ms after START_MATCH's ts (the offsets FTC Live plays its sounds at)."""
    start = t["countdown"]
    auto_end = start + t["auto"]
    teleop = auto_end + t["transition"]
    end = teleop + t["teleop"]
    out = []
    if t["countdown"] > 0:
        out.append(("countdown", 0))
    out += [("start", start), ("autoEnd", auto_end)]
    if t["transition"] > 2000:
        out.append(("controllers", auto_end + 2000))
    if t["transition"] > 3000:
        out.append(("teleop321", teleop - 3000))
    out.append(("teleop", teleop))
    if 0 < t["midmatchWarning"] < t["teleop"]:
        out.append(("midmatch", end - t["midmatchWarning"]))
    if 0 < t["endgameWarning"] < t["teleop"]:
        out.append(("endgame", end - t["endgameWarning"]))
    out.append(("end", end))
    return sorted(out, key=lambda x: x[1])


def clean_host(text):
    """"http://192.168.1.20:8080/event/x" -> "192.168.1.20:8080"; "" if not a host."""
    s = re.sub(r"^(?:https?|wss?)://", "", str(text or "").strip(), flags=re.I)
    s = re.sub(r"[/?#].*$", "", s)
    if not re.match(r"^(?:\[[0-9a-fA-F:.]+\]|[A-Za-z0-9.-]+)(?::\d{1,5})?$", s):
        return ""
    return s.lower()


def split_host(host):
    if host.startswith("["):
        h, _, rest = host[1:].partition("]")
        return h, int(rest[1:]) if rest.startswith(":") else 80
    if ":" in host:
        h, p = host.rsplit(":", 1)
        return h, int(p)
    return host, 80


def now_ms():
    return time.time() * 1000.0


def log(text):
    obs.script_log(obs.LOG_INFO, text)


# ---------------------------------------------------------------------------
# The connection (its own thread; hands messages to the OBS side through a queue)

class FeedThread(threading.Thread):
    def __init__(self, host, event, out):
        super().__init__(daemon=True)
        self.host, self.event, self.out = host, event, out
        self.stopping = threading.Event()
        self.sock = None
        self._send_lock = threading.Lock()
        self._sync_id = 0
        self._sync_sent = {}
        self._best_rtt = None

    def stop(self):
        self.stopping.set()
        s = self.sock
        if s:
            try:
                s.close()
            except OSError:
                pass

    def run(self):
        wait = RECONNECT_MIN_S
        while not self.stopping.is_set():
            self.out.put(("status", "connecting"))
            heard = False
            try:
                heard = self._session()
                if not self.stopping.is_set():
                    self.out.put(("status", "closed" if heard else "noEvent"))
            except Exception as e:  # noqa: BLE001 - any failure means retry
                if not self.stopping.is_set():
                    self.out.put(("status", "error", str(e)))
            finally:
                if self.sock:
                    try:
                        self.sock.close()
                    except OSError:
                        pass
                    self.sock = None
            if heard:
                wait = RECONNECT_MIN_S
            if self.stopping.wait(wait):
                break
            wait = min(RECONNECT_MAX_S, wait * 2)

    # -- websocket plumbing (RFC 6455, client side, text only) --

    def _send(self, opcode, data):
        payload = data.encode("utf-8") if isinstance(data, str) else data
        mask = os.urandom(4)
        n = len(payload)
        if n < 126:
            head = struct.pack("!BB", 0x80 | opcode, 0x80 | n)
        elif n < 65536:
            head = struct.pack("!BBH", 0x80 | opcode, 0x80 | 126, n)
        else:
            head = struct.pack("!BBQ", 0x80 | opcode, 0x80 | 127, n)
        body = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        with self._send_lock:
            self.sock.sendall(head + mask + body)

    def _sync(self):
        self._sync_id += 1
        self._sync_sent[self._sync_id] = now_ms()
        self._send(1, "TIMESYNC:" + json.dumps({"jsonrpc": "2.0", "id": self._sync_id, "method": "timesync"}))

    def _synced(self, reply):
        sent = self._sync_sent.pop(reply.get("id"), None)
        result = reply.get("result")
        if sent is None or not isinstance(result, (int, float)):
            return
        back = now_ms()
        rtt = back - sent
        # the quickest round trip gives the best estimate
        if self._best_rtt is None or rtt <= self._best_rtt:
            self._best_rtt = rtt
            self.out.put(("offset", result + rtt / 2 - back))

    def _session(self):
        """One connection. Returns True if any FTC Live message arrived."""
        h, p = split_host(self.host)
        s = socket.create_connection((h, p), timeout=5)
        self.sock = s
        key = base64.b64encode(os.urandom(16)).decode()
        path = "/stream/display/command/?code=" + urllib.parse.quote(self.event)
        s.sendall((f"GET {path} HTTP/1.1\r\nHost: {self.host}\r\nUpgrade: websocket\r\n"
                   f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n"
                   f"Origin: http://{self.host}\r\n\r\n").encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = s.recv(4096)
            if not chunk:
                raise ConnectionError("closed during handshake")
            buf += chunk
            if len(buf) > 65536:
                raise ConnectionError("bad handshake")
        head, _, buf = buf.partition(b"\r\n\r\n")
        status = head.split(b"\r\n", 1)[0].decode("latin-1")
        if " 101" not in status:
            raise ConnectionError(status)

        self.out.put(("status", "live"))
        s.settimeout(1.0)
        heard_any = False
        last_heard = time.monotonic()
        last_ping = time.monotonic()
        frag_op, frag = None, b""
        self._sync_sent.clear()
        self._best_rtt = None
        self._sync()

        while not self.stopping.is_set():
            # parse every complete frame in the buffer
            while len(buf) >= 2:
                b0, b1 = buf[0], buf[1]
                op, fin, masked, n = b0 & 0x0F, b0 & 0x80, b1 & 0x80, b1 & 0x7F
                i = 2
                if n == 126:
                    if len(buf) < 4:
                        break
                    n = struct.unpack("!H", buf[2:4])[0]
                    i = 4
                elif n == 127:
                    if len(buf) < 10:
                        break
                    n = struct.unpack("!Q", buf[2:10])[0]
                    i = 10
                mk = b""
                if masked:
                    mk = buf[i:i + 4]
                    i += 4
                if len(buf) < i + n:
                    break
                data = buf[i:i + n]
                buf = buf[i + n:]
                if masked:
                    data = bytes(c ^ mk[j % 4] for j, c in enumerate(data))
                if op == 8:
                    return heard_any
                if op == 9:
                    self._send(10, data)
                    continue
                if op == 10:
                    continue
                if op in (1, 2):
                    frag_op, frag = op, data
                elif op == 0:
                    frag += data
                else:
                    continue
                if not fin:
                    continue
                text = frag.decode("utf-8", "replace") if frag_op == 1 else None
                frag_op, frag = None, b""
                if text is None:
                    continue
                last_heard = time.monotonic()
                if text in ("ping", "pong") or text.startswith("APP_VERSION:"):
                    continue
                if text.startswith("TIMESYNC:"):
                    try:
                        self._synced(json.loads(text[9:]))
                    except ValueError:
                        pass
                    continue
                heard_any = True
                self.out.put(("msg", text))

            t = time.monotonic()
            if t - last_heard > SILENCE_S:
                raise ConnectionError("link went quiet")
            if t - last_ping > PING_S:
                last_ping = t
                self._send(1, "ping")
                if len(self._sync_sent) < 3:
                    self._sync()
            try:
                chunk = s.recv(65536)
            except socket.timeout:
                continue
            if not chunk:
                return heard_any
            buf += chunk
        return heard_any


# ---------------------------------------------------------------------------
# How long a video is (its own thread; FTC Live doesn't say when a video ends)

def _mp4_seconds(data):
    """Duration from an MP4/MOV 'moov' box's 'mvhd', or None."""
    i = data.find(b"mvhd")
    if i < 4 or len(data) < i + 24:
        return None
    version = data[i + 4]
    if version == 1:
        if len(data) < i + 36:
            return None
        timescale, duration = struct.unpack("!IQ", data[i + 24:i + 36])
    else:
        timescale, duration = struct.unpack("!II", data[i + 16:i + 24])
    return duration / timescale if timescale else None


def _get(url, start, length, timeout=5):
    """Bytes start..start+length-1 of url, and the file's total size if the server says."""
    req = urllib.request.Request(url, headers={"Range": f"bytes={start}-{start + length - 1}"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        total = None
        cr = r.headers.get("Content-Range") or ""
        if "/" in cr and cr.rsplit("/", 1)[1].isdigit():
            total = int(cr.rsplit("/", 1)[1])
        if r.status == 206:
            return r.read(length), total
        # no range support: the whole file comes back; read up to what we need
        total = int(r.headers.get("Content-Length") or 0) or None
        data = r.read(start + length)
        return data[start:start + length], total


def video_seconds(url):
    """The length of an MP4/MOV video at url, reading only its header boxes. None if it can't tell."""
    head, total = _get(url, 0, 65536)
    pos = 0
    while pos + 8 <= len(head):                      # walk the top-level boxes to find 'moov'
        size, kind = struct.unpack("!I4s", head[pos:pos + 8])
        if size == 1 and pos + 16 <= len(head):
            size = struct.unpack("!Q", head[pos + 8:pos + 16])[0]
        if size < 8:
            break
        if kind == b"moov":
            box = head[pos:pos + size] if pos + size <= len(head) else _get(url, pos, min(size, 4 << 20))[0]
            return _mp4_seconds(box)
        if pos + size > len(head):                    # 'moov' is further on (e.g. after the video data)
            if total and pos + size < total:
                nxt, _ = _get(url, pos + size, 16)
                if len(nxt) >= 8 and nxt[4:8] == b"moov":
                    msize = struct.unpack("!I", nxt[:4])[0]
                    return _mp4_seconds(_get(url, pos + size, min(msize, 4 << 20))[0])
            break
        pos += size
    if total and total > 65536:                       # last try: 'moov' at the very end
        tail, _ = _get(url, max(0, total - (4 << 20)), 4 << 20)
        return _mp4_seconds(tail)
    return None


def probe_video(url, key, out):
    try:
        secs = video_seconds(url)
        err = "not an MP4/MOV file" if secs is None else ""
    except Exception as e:  # noqa: BLE001 - any failure: we just won't know when it ends
        secs, err = None, str(e)
    out.put(("videoLength", key, secs, err))


# ---------------------------------------------------------------------------
# The OBS side (runs on OBS's timer)

class Switcher:
    def __init__(self):
        self.q = queue.Queue()
        self.thread = None
        self.conn = ("", "")
        self.status = "off"
        self.last = ""
        self.rules = []
        self.problem = None
        self.reset()

    def reset(self):
        self.timing = dict(DEFAULT_TIMING)
        self.offset = 0.0
        self.seen = set()
        self.scheduled = []         # [due_ms_local, trigger, field]
        self.reveal = None          # [due_ms_local, [triggers], field]
        self.delayed = None         # [due_ms_local, scene, reason]: a rule's delayed switch
        self.match_fields = {}      # match name -> field it ran on (results don't say)
        self.video = None           # the video playing on the audience display (see play_video)
        self.unknown_logged = set() # command types already reported in the log
        self.stopped_ts = float("-inf")

    # -- settings --
    def configure(self, enabled, host, event, rules):
        self.rules = rules
        want = (host, event) if enabled and host and event else ("", "")
        if want == self.conn and (self.thread or not want[0]):
            return
        self.disconnect()
        self.conn = want
        if want[0]:
            self.reset()
            self.problem = None
            self.thread = FeedThread(host, event, self.q)
            self.thread.start()
            log(f"FTC Live: listening to {event.upper()} at {host}")

    def disconnect(self):
        if self.thread:
            self.thread.stop()
            self.thread = None
        self.conn = ("", "")
        self.scheduled = []
        self.reveal = None
        self.delayed = None
        self.video = None
        self.status = "off"

    # -- local clock vs server clock --
    def local(self, server_ts):
        return float(server_ts) - self.offset

    # -- the timer --
    def tick(self):
        while True:
            try:
                item = self.q.get_nowait()
            except queue.Empty:
                break
            kind = item[0]
            if kind == "status":
                st = item[1]
                self.status = st
                # each problem is logged once, not on every retry
                if st in ("error", "noEvent") and st != self.problem:
                    self.problem = st
                    if st == "error":
                        log(f"FTC Live: can't reach {self.conn[0]} ({item[2]}), retrying")
                    else:
                        log(f"FTC Live: {self.conn[0]} closed the link; is the event code {self.conn[1].upper()} right?")
            elif kind == "offset":
                self.offset = item[1]
            elif kind == "videoLength":
                self.video_length(*item[1:])
            elif kind == "msg":
                if self.problem != "ok":
                    self.problem = "ok"
                    log("FTC Live: connected, receiving events")
                try:
                    self.handle(json.loads(item[1]))
                except (ValueError, TypeError, AttributeError):
                    pass

        t = now_ms()
        due = [x for x in self.scheduled if x[0] <= t]
        if due:
            self.scheduled = [x for x in self.scheduled if x[0] > t]
            for _, trig, field in sorted(due, key=lambda x: x[0]):
                self.fire(trig, field)
        if self.reveal and self.reveal[0] <= t:
            _, trigs, field = self.reveal
            self.reveal = None
            for trig in trigs:
                self.fire(trig, field)
        v = self.video
        if v and v["start"] is not None and v["start"] <= t:
            v["start"] = None
            self.fire("video", v["field"])
        if v and v["start"] is None and v["end"] is not None and v["end"] <= t:
            self.video = None
            self.last = f"video {v['name']} finished"
            self.fire("videoEnd", v["field"])
        if self.delayed and self.delayed[0] <= now_ms():
            _, scene, reason = self.delayed
            self.delayed = None
            switch_to(scene, reason)

    # -- one message from FTC Live --
    def handle(self, cmd):
        if not isinstance(cmd, dict) or not isinstance(cmd.get("type"), str):
            return
        p = cmd.get("params") if isinstance(cmd.get("params"), dict) else {}
        typ = cmd["type"]
        if typ == "MATCH_TIMING":
            self.timing = read_timing(p.get("matchTiming"))
            return

        key = f"{typ}-{cmd.get('index')}-{cmd.get('ts')}"
        if key in self.seen:
            return
        self.seen.add(key)
        if len(self.seen) > 500:
            self.seen = set(list(self.seen)[-200:])

        field = 0
        for f in (cmd.get("field"), p.get("field")):
            if isinstance(f, int) and f > 0:
                field = f
                break
        name = p.get("matchName") or (f"match {p['number']}" if p.get("number") is not None else "")
        # Results messages carry no field: use the field that match was shown or started on.
        if name:
            if field:
                self.match_fields[name] = field
                if len(self.match_fields) > 300:
                    self.match_fields = dict(list(self.match_fields.items())[-150:])
            else:
                field = self.match_fields.get(name, 0)
        news = not cmd.get("init")
        ts = cmd.get("ts") or 0
        # a new screen replaces a playing video: it won't "finish" on the audience display
        if self.video and typ not in UPDATE_ONLY and news:
            if self.video["start"] is None:
                log(f"FTC Live: video {self.video['name']} was replaced by {typ} before it finished")
            self.video = None

        if typ == "SHOW_PREVIEW" and news:
            self.event("preview", field, f"{name} preview")
        elif typ == "SHOW_MATCH" and news:
            self.event("show", field, f"{name} on screen")
        elif typ == "START_MATCH":
            self.start_match(cmd, field, name)
        elif typ in ("ABORT_MATCH", "CANCEL_MATCH"):
            self.stopped_ts = max(self.stopped_ts, float(ts))
            self.scheduled = []
            if typ == "ABORT_MATCH" and news:
                self.event("abort", field, f"{name} aborted")
        elif typ == "SHOW_RESULTS" and news:
            w = str(p.get("winner") or "").upper()
            rs, bs = p.get("redScore"), p.get("blueScore")
            scored = isinstance(rs, (int, float)) and isinstance(bs, (int, float))
            winner = ("redWins" if w == "RED" else "blueWins" if w == "BLUE"
                      else "tie" if (w == "TIE" or (scored and rs == bs)) else None)
            self.event("posted", field, f"{name} results")
            trigs = ["results"] + ([winner] if winner else [])
            animated = not p.get("skipAnimation") and not p.get("singleTeam")
            due = self.local(ts) + REVEAL_MS if animated else 0
            if due > now_ms():
                self.reveal = [due, trigs, field]
            else:
                self.reveal = None
                for trig in trigs:
                    self.fire(trig, field)
        elif typ == "SHOW_AWARD" and news:
            award = p.get("award") if isinstance(p.get("award"), dict) else {}
            self.event("award", field, f"award {award.get('name', '')}".strip())
        elif typ == "SHOW_SELECTION" and news:
            self.event("selection", field, "alliance selection")
        elif typ == "SHOW_VIDEO" and news:
            self.event("videoScreen", field, "video screen")
        elif typ == "PLAY_VIDEO" and news:
            self.play_video(p.get("videoUrl"), field, ts)
        elif typ == "SHOW_MESSAGE" and news:
            text = p.get("message") if isinstance(p.get("message"), str) else ""
            self.event("message", field, f"message {text.strip()[:60]!r}" if text.strip() else "message")

        if typ not in KNOWN_COMMANDS and typ not in self.unknown_logged:
            self.unknown_logged.add(typ)
            keys = ", ".join(sorted(p.keys())[:8])
            log(f"FTC Live: display command {typ} (params: {keys or 'none'})")

    # -- a video on the audience display --
    def play_video(self, url, field, ts):
        url = str(url or "")
        name = urllib.parse.unquote(url.rsplit("/", 1)[-1].split("?")[0]) or "video"
        began = self.local(ts) + VIDEO_START_MS          # when the display starts playing it
        key = f"{url}@{ts}"
        self.last = f"video {name}"
        self.video = {"key": key, "name": name, "field": field, "began": began,
                      "start": max(began, now_ms()), "end": None}
        # FTC Live never says when a video ends: read its length from the file itself
        if url and self.conn[0] and any(r["trigger"] == "videoEnd" and r["scene"] for r in self.rules):
            full = url if re.match(r"^https?://", url, re.I) else f"http://{self.conn[0]}/{url.lstrip('/')}"
            threading.Thread(target=probe_video, args=(full, key, self.q), daemon=True).start()

    def video_length(self, key, secs, err):
        v = self.video
        if not v or v["key"] != key:
            return
        if secs is None:
            log(f"FTC Live: can't tell how long video {v['name']} is ({err}); 'Video finished' won't fire for it")
            return
        v["end"] = v["began"] + secs * 1000 + VIDEO_AFTER_MS
        log(f"FTC Live: video {v['name']} is {secs:.1f}s long")

    def event(self, trig, field, text):
        self.last = text
        self.fire(trig, field)

    def start_match(self, cmd, field, name):
        # the replay on connecting isn't in order: a start an abort came after is over
        if cmd.get("init") and self.stopped_ts >= (cmd.get("ts") or 0):
            return
        self.scheduled = []
        self.reveal = None
        t0 = self.local(cmd.get("ts") or 0)
        t = now_ms()
        if not cmd.get("init"):
            self.last = f"{name} started"
        for trig, at in phases(self.timing):
            wait = t0 + at - t
            if wait < -LATE_MS:
                continue
            if wait <= 0:
                if not cmd.get("init"):
                    self.fire(trig, field)
                continue
            self.scheduled.append([t0 + at, trig, field])

    # -- switching --
    def rule_for(self, trig, field):
        """A rule for this exact field beats an "any field" rule."""
        any_field = None
        for r in self.rules:
            if r["trigger"] != trig or not r["scene"]:
                continue
            if r["field"] and r["field"] == field:
                return r
            if not r["field"] and any_field is None:
                any_field = r
        return any_field

    def fire(self, trig, field):
        rule = self.rule_for(trig, field)
        where = f" (field {field})" if field else " (no field)"
        reason = f"{TRIGGER_LABELS.get(trig, trig)}{where}"
        if not rule:
            if any(r["trigger"] == trig and r["scene"] for r in self.rules):
                log(f"FTC Live: {reason}: no rule matches this field, nothing switched")
            return False
        # a newer trigger replaces a delayed switch that hasn't happened yet
        if self.delayed:
            log(f"FTC Live: {reason} replaced the waiting switch to {self.delayed[1]}")
            self.delayed = None
        delay = rule.get("delay") or 0
        if delay > 0:
            self.delayed = [now_ms() + delay * 1000, rule["scene"], f"{reason}, after {delay:g}s"]
            log(f"FTC Live: {reason} -> {rule['scene']} in {delay:g}s")
        else:
            switch_to(rule["scene"], reason)
        return True


def switch_to(scene_name, reason=""):
    """Put a scene on program. In studio mode OBS transitions it to program."""
    src = obs.obs_get_source_by_name(scene_name)
    if not src:
        log(f"FTC Live: scene '{scene_name}' doesn't exist ({reason})")
        return
    try:
        cur = obs.obs_frontend_get_current_scene()
        cur_name = obs.obs_source_get_name(cur) if cur else None
        if cur:
            obs.obs_source_release(cur)
        if cur_name != scene_name:
            obs.obs_frontend_set_current_scene(src)
            log(f"FTC Live: {reason} -> {scene_name}")
    finally:
        obs.obs_source_release(src)


SW = Switcher()
_events_cache = []
_settings = None


# ---------------------------------------------------------------------------
# OBS script entry points

def script_description():
    return ("<b>FTC Live Scene Switcher</b><br>Switches the program scene on FTC Live match events. "
            "Add a rule per trigger, optionally tied to a field, with an optional delay. A field rule beats an "
            "'Any field' rule, and a newer trigger cancels a delayed switch that hasn't happened yet.")


def read_rules(settings):
    n = max(0, min(MAX_RULES, obs.obs_data_get_int(settings, "rule_count")))
    rules = []
    for i in range(1, n + 1):
        rules.append({
            "trigger": obs.obs_data_get_string(settings, f"rule{i}_trigger"),
            "field": obs.obs_data_get_int(settings, f"rule{i}_field"),
            "scene": obs.obs_data_get_string(settings, f"rule{i}_scene"),
            "delay": max(0.0, obs.obs_data_get_double(settings, f"rule{i}_delay")),
        })
    return rules


def script_defaults(settings):
    obs.obs_data_set_default_bool(settings, "enabled", True)
    obs.obs_data_set_default_int(settings, "rule_count", 2)
    obs.obs_data_set_default_string(settings, "rule1_trigger", "show")
    obs.obs_data_set_default_int(settings, "rule1_field", 1)
    obs.obs_data_set_default_string(settings, "rule2_trigger", "show")
    obs.obs_data_set_default_int(settings, "rule2_field", 2)


def script_update(settings):
    global _settings
    _settings = settings
    host = clean_host(obs.obs_data_get_string(settings, "host"))
    event = obs.obs_data_get_string(settings, "event").strip().lower()
    SW.configure(obs.obs_data_get_bool(settings, "enabled"), host, event, read_rules(settings))


def _refresh_events(props, prop):
    global _events_cache
    host = clean_host(obs.obs_data_get_string(_settings, "host")) if _settings else ""
    if not host:
        log("FTC Live: enter the FTC Live server address first")
        return False
    try:
        with urllib.request.urlopen(f"http://{host}/api/v1/events/", timeout=3) as r:
            codes = json.loads(r.read().decode()).get("eventCodes") or []
        _events_cache = [str(c).lower() for c in codes]
        log(f"FTC Live: {host} has {len(_events_cache)} event(s): {', '.join(c.upper() for c in _events_cache) or 'none'}")
        if len(_events_cache) == 1 and not obs.obs_data_get_string(_settings, "event"):
            obs.obs_data_set_string(_settings, "event", _events_cache[0])
            script_update(_settings)
    except Exception as e:  # noqa: BLE001
        log(f"FTC Live: couldn't reach {host} ({e})")
    return True


def _refresh(props, prop):
    return True


def _rule_count_changed(props, prop, settings):
    n = obs.obs_data_get_int(settings, "rule_count")
    for i in range(1, MAX_RULES + 1):
        obs.obs_property_set_visible(obs.obs_properties_get(props, f"rule{i}"), i <= n)
    return True


def _test_rule(i):
    def cb(props, prop):
        if not _settings:
            return False
        scene = obs.obs_data_get_string(_settings, f"rule{i}_scene")
        if scene:
            switch_to(scene, f"test of rule {i}")
        return False
    return cb


def script_properties():
    props = obs.obs_properties_create()

    status = {"off": "Off", "connecting": "Connecting...", "live": "Connected",
              "error": "Can't reach the server, retrying", "noEvent": "Server closed the link (wrong event code?)",
              "closed": "Reconnecting..."}.get(SW.status, SW.status)
    if SW.last:
        status += f"  |  Last: {SW.last}"
    obs.obs_properties_add_text(props, "status_info", f"Status: {status}", obs.OBS_TEXT_INFO)

    obs.obs_properties_add_bool(props, "enabled", "Enabled")
    obs.obs_properties_add_text(props, "host", "FTC Live server (IP)", obs.OBS_TEXT_DEFAULT)
    ev = obs.obs_properties_add_list(props, "event", "Event code", obs.OBS_COMBO_TYPE_EDITABLE,
                                     obs.OBS_COMBO_FORMAT_STRING)
    for c in _events_cache:
        obs.obs_property_list_add_string(ev, c.upper(), c)
    obs.obs_properties_add_button(props, "find_events", "Find events on server", _refresh_events)
    obs.obs_properties_add_button(props, "refresh", "Refresh status and scene list", _refresh)

    rc = obs.obs_properties_add_int(props, "rule_count", "Number of rules", 1, MAX_RULES, 1)
    obs.obs_property_set_modified_callback(rc, _rule_count_changed)

    scenes = obs.obs_frontend_get_scene_names() or []
    n = obs.obs_data_get_int(_settings, "rule_count") if _settings else 2
    for i in range(1, MAX_RULES + 1):
        g = obs.obs_properties_create()
        t = obs.obs_properties_add_list(g, f"rule{i}_trigger", "When", obs.OBS_COMBO_TYPE_LIST,
                                        obs.OBS_COMBO_FORMAT_STRING)
        obs.obs_property_list_add_string(t, "(off)", "")
        for tid, label in TRIGGERS:
            obs.obs_property_list_add_string(t, label, tid)
        f = obs.obs_properties_add_list(g, f"rule{i}_field", "Field", obs.OBS_COMBO_TYPE_LIST,
                                        obs.OBS_COMBO_FORMAT_INT)
        obs.obs_property_list_add_int(f, "Any field", 0)
        for k in range(1, MAX_FIELDS + 1):
            obs.obs_property_list_add_int(f, f"Field {k}", k)
        s = obs.obs_properties_add_list(g, f"rule{i}_scene", "Switch to", obs.OBS_COMBO_TYPE_LIST,
                                        obs.OBS_COMBO_FORMAT_STRING)
        obs.obs_property_list_add_string(s, "(choose a scene)", "")
        for name in scenes:
            obs.obs_property_list_add_string(s, name, name)
        saved = obs.obs_data_get_string(_settings, f"rule{i}_scene") if _settings else ""
        if saved and saved not in scenes:
            obs.obs_property_list_add_string(s, f"{saved} (missing)", saved)
        obs.obs_properties_add_float(g, f"rule{i}_delay", "Delay (seconds)", 0.0, 600.0, 0.5)
        obs.obs_properties_add_button(g, f"rule{i}_test", "Test (switch now)", _test_rule(i))
        grp = obs.obs_properties_add_group(props, f"rule{i}", f"Rule {i}", obs.OBS_GROUP_NORMAL, g)
        obs.obs_property_set_visible(grp, i <= n)
    return props


def _tick():
    SW.tick()


def script_load(settings):
    obs.timer_add(_tick, 50)


def script_unload():
    obs.timer_remove(_tick)
    SW.disconnect()

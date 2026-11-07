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
]
TRIGGER_LABELS = dict(TRIGGERS)

MAX_RULES = 20
MAX_FIELDS = 6

RECONNECT_MIN_S = 1.0
RECONNECT_MAX_S = 10.0
PING_S = 10.0
SILENCE_S = 25.0
LATE_MS = 1500        # a period that began this recently still fires
REVEAL_MS = 4000      # results video length + fade, as measured for the DMX controller

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
        news = not cmd.get("init")
        ts = cmd.get("ts") or 0

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
    def scene_for(self, trig, field):
        """A rule for this exact field beats an "any field" rule."""
        any_field = None
        for r in self.rules:
            if r["trigger"] != trig or not r["scene"]:
                continue
            if r["field"] and r["field"] == field:
                return r["scene"]
            if not r["field"] and any_field is None:
                any_field = r["scene"]
        return any_field

    def fire(self, trig, field):
        scene = self.scene_for(trig, field)
        where = f" (field {field})" if field else ""
        if not scene:
            return False
        switch_to(scene, f"{TRIGGER_LABELS.get(trig, trig)}{where}")
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
            "Add a rule per trigger, optionally tied to a field. A field rule beats an 'Any field' rule.")


def read_rules(settings):
    n = max(0, min(MAX_RULES, obs.obs_data_get_int(settings, "rule_count")))
    rules = []
    for i in range(1, n + 1):
        rules.append({
            "trigger": obs.obs_data_get_string(settings, f"rule{i}_trigger"),
            "field": obs.obs_data_get_int(settings, f"rule{i}_field"),
            "scene": obs.obs_data_get_string(settings, f"rule{i}_scene"),
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

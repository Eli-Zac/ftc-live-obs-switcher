# FTC Live OBS Scene Switcher

An OBS Studio script that switches the program scene when things happen in FTC Live (FIRST Tech Challenge scoring): a match comes up on screen, starts, hits endgame, ends, results are posted, and more. Rules can be tied to a field, so a match on field 1 switches to your Field 1 scene and field 2 to another.

It listens to the same feed FTC Live's audience display uses. That feed needs no login, and the script only listens: it never changes anything on the scoring server.

## Setup

1. Install 64-bit [Python 3](https://www.python.org/downloads/) if it isn't already on the streaming PC.
2. In OBS: **Tools → Scripts → Python Settings**, and point it at your Python install folder.
3. On the **Scripts** tab, click **+** and select [`ftc_live_scene_switcher.py`](ftc_live_scene_switcher.py).
4. Enter the FTC Live server's IP (e.g. `192.168.1.20`), click **Find events on server**, and pick the event.
5. Add rules.

## Rules

Each rule is **When** (a trigger) + **Field** (Any field, or Field 1 to 6) + **Switch to** (a scene). Use **Test** on a rule to switch to its scene straight away.

A field-specific rule beats an "Any field" rule for the same trigger. For example:

| When | Field | Switch to |
|---|---|---|
| Match on screen | Field 1 | Field 1 |
| Match on screen | Field 2 | Field 2 |
| Results shown - any | Any field | Results |

### Triggers

| Trigger | When it fires |
|---|---|
| Match preview | The next match is put up on the audience display |
| Match on screen | The match screen comes up, before it starts |
| Countdown | START pressed, the countdown before the match |
| Match start (auto) | Autonomous begins |
| Auto ends | End of autonomous |
| Pick up controllers | Two seconds after auto ends |
| 3-2-1 to driver-controlled | Three seconds before driver-controlled |
| Driver-controlled | Driver-controlled begins |
| Mid-match warning | Halfway through driver-controlled |
| Endgame | Endgame begins |
| Match end (buzzer) | The buzzer |
| Match aborted | The match is stopped early |
| Results posted (instant) | The moment the scorekeeper posts, as the alliance animation starts |
| Results shown - any / red wins / blue wins / tie | The scores appear, after the animation |
| Award presented | An award is put up on the display |
| Alliance selection | Alliance selection comes up on the display |

The in-match phases aren't sent by FTC Live as separate messages. Like FTC Live's own display, the script works them out from the match start time and the event's match timing, after syncing its clock with the server.

## Behaviour

- **Studio mode:** the scene is transitioned to program, using your current transition.
- **Joining mid-match:** it picks up from the next phase and doesn't replay ones that already happened.
- **Dropouts:** it reconnects on its own.
- **Logging:** every switch and connection problem is written to **Script Log**.

Tested against FTC Live v8.0.3 message formats.

# pipecat-bot-speaking-observer

A [Pipecat](https://github.com/pipecat-ai/pipecat) `FrameProcessor` that watches `BotStartedSpeakingFrame` / `BotStoppedSpeakingFrame` and hands you the turn-boundary callbacks you need to drive a turn gate.

~80 lines.

## The problem

Voice agents over WebSocket lack server-side echo cancellation. If you let the user-speaking lane stay open while the bot is talking, the bot's own TTS audio bleeds back into the mic, the STT picks it up as user speech, and the conversation goes off the rails.

The fix is a **turn gate**: while the bot speaks, the user lane is closed; when the bot stops, you wait briefly for the client to drain its audio buffer, then reopen. Pipecat's `ExternalUserTurnStrategies` mode wants you to *explicitly* push `UserStartedSpeakingFrame` upstream when it's safe — but figuring out *when* is the work this observer does for you.

## Install

```bash
pip install pipecat-bot-speaking-observer
```

## Wire it up

Position: **after `transport.output()`** in the pipeline. From that seat, the observer sees bot-speaking boundaries exactly as the client perceives them.

```python
from pipecat.pipeline.pipeline import Pipeline
from pipecat_bot_speaking_observer import BotSpeakingObserver

async def disable_user_lane():
    await rtvi.send_server_message({"type": "bot:user_turn_disabled"})

async def arm_fallback_timer():
    # If the client doesn't send client:playback_drained within 5s,
    # force the gate open and ship a bot:user_turn_enabled message.
    asyncio.create_task(_fallback(5.0))

observer = BotSpeakingObserver(
    on_bot_started_speaking=disable_user_lane,
    on_bot_stopped_speaking=arm_fallback_timer,
)

pipeline = Pipeline([
    transport.input(),
    stt,
    context_aggregator.user(),
    llm,
    tts,
    transport.output(),
    observer,                  # ← here
    context_aggregator.assistant(),
])
```

## What the observer does for you

On `BotStoppedSpeakingFrame`, the observer always pushes a `UserStartedSpeakingFrame` upstream. This is the **explicit signal** that Pipecat's `ExternalUserTurnStrategies` mode requires to open the user-speaking aggregator window. Without it, the aggregator stays closed forever and user transcripts are dropped.

Your `on_bot_stopped_speaking` callback is where you start the *fallback* timer that closes the gate again if the client misbehaves (doesn't send `client:playback_drained`, drops the WS, etc.).

## Related projects

- 🎯 [`pipecat-sarvam-azure-starter`](https://github.com/dpkdhingra91/pipecat-sarvam-azure-starter) — canonical Sarvam + Azure voice pipeline this observer was extracted from.
- 🛡️ [`pipecat-content-filter-fallback`](https://github.com/dpkdhingra91/pipecat-content-filter-fallback) — pairs in the same pipeline.
- 📊 [`pipecat-outbound-audio-counter`](https://github.com/dpkdhingra91/pipecat-outbound-audio-counter) — diagnostic sibling for TTS silent-failure detection.

## License

MIT — see [LICENSE](LICENSE).

---

*Extracted from the production voice stack of [AI Interview Agents](https://www.aiinterviewagents.com) — an AI voice interviewer that runs real two-way spoken interviews and screens candidates at scale.*

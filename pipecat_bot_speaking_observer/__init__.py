"""pipecat-bot-speaking-observer — turn-gate orchestration for Pipecat voice agents.

Watches `BotStartedSpeakingFrame` / `BotStoppedSpeakingFrame` and drives a
"turn gate" — a small state machine that disables the user-speaking lane
while the bot is talking, then re-enables it (with an optional fallback
timer) when the bot finishes.

Solves the "user starts talking over the bot" problem in WS-only voice
agents that lack server-side echo cancellation.
"""

import asyncio
import logging
from typing import Awaitable, Callable, Optional

from pipecat.frames.frames import (
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    Frame,
    UserStartedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor

logger = logging.getLogger(__name__)


class BotSpeakingObserver(FrameProcessor):
    """Observe bot-speaking boundaries to drive a turn gate + fallback timer.

    Placed **after** `transport.output()` in the pipeline so the frames it
    sees match what the client perceives as bot-speaking boundaries.

    Lifecycle per turn:
        BotStartedSpeakingFrame (DOWNSTREAM) →
          on_bot_started_speaking() called
          (your gate: disable user lane, send "bot speaking" message to client)

        BotStoppedSpeakingFrame (DOWNSTREAM) →
          on_bot_stopped_speaking() called
          (your gate: arm a fallback timer; if client doesn't acknowledge
           playback drained within N seconds, force the gate open)
          UserStartedSpeakingFrame pushed UPSTREAM
          (with ExternalUserTurnStrategies, the user aggregator only opens
           on this explicit signal — without it, candidate STT stays ignored)

    Args:
        on_bot_started_speaking: optional async callable, invoked on each
            bot start. Your code does whatever — disable input, send a
            server-message to the client, log, etc.
        on_bot_stopped_speaking: optional async callable, invoked on each
            bot stop. Typical use: arm a fallback timer to force the gate
            open if the client never sends `client:playback_drained`.
    """

    def __init__(
        self,
        on_bot_started_speaking: Optional[Callable[[], Awaitable[None]]] = None,
        on_bot_stopped_speaking: Optional[Callable[[], Awaitable[None]]] = None,
    ):
        super().__init__()
        self._on_started = on_bot_started_speaking
        self._on_stopped = on_bot_stopped_speaking

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        await super().process_frame(frame, direction)

        if isinstance(frame, BotStartedSpeakingFrame) and direction == FrameDirection.DOWNSTREAM:
            logger.debug("[turn_gate] bot_started")
            if self._on_started is not None:
                try:
                    await self._on_started()
                except Exception:
                    logger.exception("on_bot_started_speaking raised — continuing")

        elif isinstance(frame, BotStoppedSpeakingFrame) and direction == FrameDirection.DOWNSTREAM:
            logger.debug("[turn_gate] bot_stopped")
            if self._on_stopped is not None:
                try:
                    await self._on_stopped()
                except Exception:
                    logger.exception("on_bot_stopped_speaking raised — continuing")

            # Open the user aggregator's listening window by pushing
            # UserStartedSpeakingFrame upstream. With ExternalUserTurnStrategies,
            # the aggregator only accepts this explicit signal — it won't derive
            # it from VAD or transcription alone. Without this push, candidate
            # transcriptions are ignored forever.
            try:
                await self.push_frame(UserStartedSpeakingFrame(), FrameDirection.UPSTREAM)
            except Exception:
                logger.exception("Failed to push UserStartedSpeakingFrame upstream")

        await self.push_frame(frame, direction)


__all__ = ["BotSpeakingObserver"]
__version__ = "0.1.0"

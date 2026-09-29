"""Event controller."""

import asyncio
import contextlib
import json
import logging
import random
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal, NoReturn

import aiohttp

from ..const import API_URL_BASE
from ..events import EventBrokerMessage, EventBrokerTopic
from ..exceptions import (
    AuthenticationFailed,
    NotInitialized,
    OtpRequired,
    ServiceUnavailable,
    SessionExpired,
    UnexpectedResponse,
)
from .messages import (
    UNDEFINED,
    BaseWSMessage,
    EventWSMessage,
    PropertyChangeWSMessage,
    ResourceEventType,
    ResourcePropertyChangeType,
    WebSocketMessageTester,
)

if TYPE_CHECKING:
    from .. import AlarmBridge


ALL_TOKEN = "*"  # noqa: S105
ALL_TOKEN_T = Literal["*"]


KEEP_ALIVE_SIGNAL_INTERVAL_S = 60
MAX_RECONNECT_WAIT_S = 30 * 60
DEFAULT_SIGNALS_PER_SESSION_REFRESH = 1
DEFAULT_MAX_CONNECTION_ATTEMPTS = 25
MAX_KEEPALIVE_FAILURES = 3  # Number of keepalive failures before forcing reconnection


log = logging.getLogger(__name__)


class WebSocketState(Enum):
    """Enum with possible Events."""

    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    DEAD = "dead"
    WAITING = "waiting"
    AUTH_REQUIRED = "auth_required"  # Needs re-authentication (e.g., OTP required)

    # Only for emit
    RECONNECTED = "reconnected"


@dataclass(kw_only=True)
class RawResourceEventMessage(EventBrokerMessage):
    """Message class for updated resources."""

    topic: EventBrokerTopic = EventBrokerTopic.RAW_RESOURCE_EVENT
    ws_message: BaseWSMessage


@dataclass(kw_only=True)
class ConnectionEvent(EventBrokerMessage):
    """Message class for updated resources."""

    topic: EventBrokerTopic = EventBrokerTopic.CONNECTION_EVENT
    current_state: WebSocketState
    next_attempt_s: int | None = None


@dataclass
class SupportedResourceEvents:
    """Supported WebSocket Notifications."""

    state_change: bool = False
    geofence_crossing: bool = False
    events: list[ResourceEventType | ALL_TOKEN_T] = field(default_factory=list)
    property_changes: list[ResourcePropertyChangeType | ALL_TOKEN_T] = field(default_factory=list)


class WebSocketClient:
    """Control WebSocket connection and distribute messages."""

    def __init__(
        self,
        bridge: "AlarmBridge",
        max_connection_attempts: int = DEFAULT_MAX_CONNECTION_ATTEMPTS,
        infinite_retry: bool = False,
    ) -> None:
        """Initialize authentication controller.

        Args:
            bridge: The AlarmBridge instance
            max_connection_attempts: Maximum reconnection attempts before giving up (default 25)
            infinite_retry: If True, never give up on reconnection attempts
        """
        self._bridge = bridge
        self._max_connection_attempts = max_connection_attempts
        self._infinite_retry = infinite_retry

        self._token: str | None = None
        self._ws_endpoint: str | None = None

        self._state = WebSocketState.DISCONNECTED

        self._event_queue: asyncio.Queue = asyncio.Queue()
        self._background_tasks: set[asyncio.Task] = set()

        self._last_session_refresh: datetime | None = None
        self._session_refresh_interval_ms: int | None = None
        self._keep_alive_url: str | None = None
        self._event_history: deque = deque(maxlen=25)

        self._initialized = False

        # Track connection health metrics
        self._connect_attempts = 0
        self._keepalive_failures = 0
        self._last_message_time: float | None = None
        self._connected_at: float | None = None
        self._force_reconnect = False
        self._current_websocket: aiohttp.ClientWebSocketResponse | None = None

    @property
    def connected(self) -> bool:
        """Whether client is connected to server."""

        return self.state in [WebSocketState.CONNECTED, WebSocketState.RECONNECTED]

    @property
    def state(self) -> WebSocketState:
        """Return connection state."""

        return self._state

    @property
    def last_events(self) -> list[dict]:
        """Return a list with the previous X messages."""

        return list(self._event_history)

    def is_healthy(self) -> bool:
        """Check if WebSocket connection is healthy.

        Returns True if:
        - Connection is in CONNECTED state
        - We've received a message within the last 5 minutes
        """
        if self._state != WebSocketState.CONNECTED:
            return False

        # Check if we've received a message recently (within 5 minutes)
        if self._last_message_time is not None:
            seconds_since_last = time.time() - self._last_message_time
            if seconds_since_last > 300:
                log.debug(
                    "[HEALTH CHECK] Connection unhealthy: no messages for %.1f seconds (threshold: 300s)",
                    seconds_since_last,
                )
                return False

        return True

    async def _force_close_websocket(self) -> None:
        """Force close the current WebSocket connection to trigger reconnection."""
        if self._current_websocket is not None and not self._current_websocket.closed:
            log.info("[WEBSOCKET] Force closing WebSocket connection")
            await self._current_websocket.close()

    @property
    def connection_info(self) -> dict:
        """Get detailed connection status for debugging.

        Returns a dictionary with:
        - state: Current WebSocket state
        - connected_at: Timestamp when connection was established
        - last_message: Timestamp of last received message
        - reconnect_attempts: Number of reconnection attempts
        - keepalive_failures: Number of consecutive keepalive failures
        - is_healthy: Overall health status
        """
        return {
            "state": self._state.name if self._state else "UNKNOWN",
            "connected_at": self._connected_at,
            "last_message": self._last_message_time,
            "reconnect_attempts": self._connect_attempts,
            "keepalive_failures": self._keepalive_failures,
            "is_healthy": self.is_healthy(),
            "max_connection_attempts": self._max_connection_attempts,
            "infinite_retry": self._infinite_retry,
        }

    async def initialize(self) -> None:
        """
        Start listening for events.

        Connection will be auto-reconnected if it gets lost.
        """

        if not self._bridge.initialized:
            raise NotInitialized

        if self._initialized:
            return

        def emergency_stop(task: asyncio.Task) -> None:
            """Stop all background tasks and state reason."""

            if task.cancelled():
                log.debug("WebSocket client task %s was killed.", task.get_name())
            else:
                log.error(
                    "WebSocket client ran into an error with the %s task. Killing siblings.",
                    task.get_name(),
                )
                self.stop(WebSocketState.DEAD)

        if len(self._background_tasks) > 0:
            raise RuntimeError("Already initialized")

        def add_task(coro: Any, name: str) -> asyncio.Task:
            """Create a task and add it to the background tasks set."""
            task = asyncio.create_task(coro, name=name)
            self._background_tasks.add(task)
            task.add_done_callback(emergency_stop)
            task.add_done_callback(self._background_tasks.discard)
            return task

        add_task(self._event_reader(), "Event Reader")
        add_task(self._event_processor(), "Event Processor")
        add_task(self._keep_alive(), "Keep Alive")

        self._initialized = True

    def stop(self, state: WebSocketState = WebSocketState.DISCONNECTED) -> None:
        """Stop listening for events."""

        self._set_state(state)

        for task in list(self._background_tasks):
            task.cancel()

        self._background_tasks.clear()

        self._initialized = False

    #############################
    # NOTIFICATION TRANSMISSION #
    #############################

    def _emit_ws_state(self, state: WebSocketState, next_attempt_s: int | None = None) -> None:
        """Emit connection event to all listeners."""

        self._bridge.events.publish(ConnectionEvent(current_state=state, next_attempt_s=next_attempt_s))

    def _emit_resource(self, data: BaseWSMessage) -> None:
        """Emit resource event to all listeners."""

        self._bridge.events.publish(RawResourceEventMessage(ws_message=data))

    #################################
    # LONG-RUNNING BACKGROUND TASKS #
    #################################

    async def _event_reader(self) -> NoReturn:
        """Maintain connection with server and read events from stream."""

        self._set_state(WebSocketState.CONNECTING)
        connect_attempts = 0

        while True:
            connect_attempts += 1
            self._connect_attempts = connect_attempts

            # Check if force reconnect was requested (e.g., by keepalive failures)
            if self._force_reconnect:
                self._force_reconnect = False
                seconds_since_msg = (
                    time.time() - self._last_message_time if self._last_message_time else None
                )
                log.warning(
                    "[EVENT READER] Force reconnect requested (last_msg=%.0fs ago, attempt=%d)",
                    seconds_since_msg or 0,
                    connect_attempts,
                )

            try:
                await self._authenticate()

                log.info("[EVENT READER] Connecting to Alarm.com WebSocket endpoint...")

                async with self._bridge.ws_connect(f"{self._ws_endpoint}/?f=1&auth={self._token}") as websocket:
                    self._current_websocket = websocket
                    self._set_state(
                        WebSocketState.CONNECTED if connect_attempts == 1 else WebSocketState.RECONNECTED,
                    )
                    connect_attempts = 1
                    self._connect_attempts = 1
                    self._keepalive_failures = 0  # Reset on successful connection
                    self._connected_at = time.time()

                    log.info(
                        "[EVENT READER] Connected to WebSocket (attempt=%d, state=%s)",
                        connect_attempts,
                        self._state.name,
                    )

                    async for msg in websocket:
                        # Track last message time for health checks
                        self._last_message_time = time.time()

                        if msg.type == aiohttp.WSMsgType.CLOSED:
                            log.warning(
                                "[EVENT READER] WebSocket connection closed by server: Code: %s, Message: '%s'. Initiating reconnection.",
                                msg.data,
                                msg.extra,
                            )
                            break  # Exit loop to trigger reconnection

                        if msg.type == aiohttp.WSMsgType.ERROR:
                            log.warning("[EVENT READER] WebSocket error received: '%s'. Initiating reconnection.", msg.data)
                            break  # Exit loop to trigger reconnection

                        if msg.type != aiohttp.WSMsgType.TEXT:
                            log.debug(
                                "[EVENT READER]Got non-text WebSocket message: '%s'",
                                msg.data,
                            )
                            continue

                        self._event_queue.put_nowait(msg.data)
                        self._event_history.append(msg.data)

                # Clear websocket reference after exiting async with block
                self._current_websocket = None

                # Log connection duration when exiting
                if self._connected_at:
                    connection_duration = time.time() - self._connected_at
                    log.info(
                        "[EVENT READER] WebSocket loop exited after %.1f seconds (close_code=%s)",
                        connection_duration,
                        websocket.close_code,
                    )

                if log.level < logging.DEBUG:
                    close_code: aiohttp.WSCloseCode | int | None = websocket.close_code
                    with contextlib.suppress(AttributeError):
                        if websocket.close_code:
                            # TODO: Close code 1008 means rejected token. Adc web portal initiated immediate
                            # reconnect.
                            close_code = aiohttp.WSCloseCode(int(websocket.close_code))

                    log.debug("[EVENT READER] WebSocket Connection Closed (%s)", close_code)

            except OtpRequired:
                # Handle OTP gracefully - don't hard-fail, trigger re-authentication
                log.warning(
                    "[EVENT READER] Server requested OTP - this may indicate an expired MFA token. "
                    "Triggering re-authentication event."
                )
                self._emit_ws_state(WebSocketState.AUTH_REQUIRED, None)
                # Don't raise - continue reconnection loop to allow potential recovery
                # after user re-authenticates through the integration
            except (AuthenticationFailed, SessionExpired):
                # Token request failed.
                log.debug(
                    "[EVENT READER] Failed to authenticate WebSocket connection. This is likely due to a session timeout."
                )
            except (
                TimeoutError,
                aiohttp.ClientError,
                UnexpectedResponse,
                aiohttp.ClientConnectionError,
            ) as err:
                # status = 401 (HTTP):                      WebSocket Authentication failure
                # errno = 104 (Socket errno.ECONNRESET):    Connection reset by peer
                log.debug(
                    "[EVENT READER] Encountered WebSocket error. Attempting to recover.\nHTTP STATUS: %s\nSOCKET ERROR: %s %s",
                    getattr(err, "status", None),
                    getattr(err, "errno", None),
                    getattr(err, "strerror", None),
                )
            except Exception:
                # for debugging purpose only
                log.exception("[EVENT READER] Fatal Error")
                raise

            # Check if we should stop trying (respecting infinite_retry option)
            if not self._infinite_retry and connect_attempts >= self._max_connection_attempts:
                log.error(
                    "[EVENT READER] Hit max retries (%s), giving up on WebSocket connection",
                    self._max_connection_attempts,
                )
                self.stop()

            reconnect_wait = round(
                min(10 * connect_attempts * random.random(), MAX_RECONNECT_WAIT_S)  # noqa: S311
            )

            log.debug(
                "[EVENT READER] WebSocket Disconnected - Reconnect attempt %s of %s will be attempted in %s seconds.",
                connect_attempts,
                self._max_connection_attempts if not self._infinite_retry else "∞",
                reconnect_wait,
            )

            self._set_state(WebSocketState.DISCONNECTED, reconnect_wait)

            # every 10 failed connect attempts log warning
            if connect_attempts % 10 == 0:
                log.warning(
                    "[EVENT READER] %s attempts to (re)connect Alarm.com WebSocket endpoint failed.",
                    connect_attempts,
                )

            self._set_state(WebSocketState.WAITING)

            await asyncio.sleep(reconnect_wait)

    async def _event_processor(self) -> NoReturn:
        """Process incoming events."""

        while True:
            try:
                msg_json: str = str(await self._event_queue.get())

                msg_tester = WebSocketMessageTester.from_json(msg_json)

                # log.debug("Tester message: %s", msg_tester)

                converted_message: BaseWSMessage | None = None

                log.debug("[EVENT PROCESSOR] Received WebSocket Message: %s", msg_json)

                # Determine and set message type class.
                # Skipped message types seem to be unused by Alarm.com's webapp. The same actions
                # are instead handled via event messages.

                if UNDEFINED not in [
                    msg_tester.event_type,
                    msg_tester.event_value,
                    msg_tester.qstring_for_extra_data,
                    msg_tester.event_date_utc,
                ]:
                    converted_message = EventWSMessage.from_json(msg_json)
                elif UNDEFINED not in [
                    msg_tester.event_type,
                    msg_tester.correlated_id,
                ]:
                    # converted_message = MonitoringEventWSMessage.from_json(msg_json)
                    # These messages will be picked up as EventWSMessage, and that's fine.
                    # Device WS controllers will need to interpret them the same way.
                    continue
                elif UNDEFINED not in [msg_tester.property_, msg_tester.property_value]:
                    converted_message = PropertyChangeWSMessage.from_json(msg_json)
                elif UNDEFINED not in [msg_tester.fence_id, msg_tester.is_inside_now]:
                    # converted_message = GeofenceCrossingWSMessage.from_json(msg_json)
                    continue
                elif UNDEFINED not in [msg_tester.new_state, msg_tester.flag_mask]:
                    # converted_message = StatusUpdateWSMessage.from_json(msg_json)
                    continue

                log.debug(
                    "[EVENT PROCESSOR] WebSocket message type identified as %s",
                    converted_message.__class__.__name__,
                )

                if converted_message:
                    self._emit_resource(converted_message)
                else:
                    log.warning(
                        "[EVENT PROCESSOR] Unprocessable message received: %s",
                        json.loads(msg_json),
                    )

            except Exception:
                log.exception("[EVENT PROCESSOR] Failed to convert message.\n")
                try:
                    log.debug(json.loads(msg_json))
                except (json.JSONDecodeError, TypeError):
                    log.debug("[EVENT PROCESSOR] Raw message: %s", msg_json)

    async def _keep_alive(self) -> NoReturn:
        """
        Keep session alive.

        Alarm.com's webapp uses the keep alive to handle session timeouts. We'll use the event reader to do that, instead.
        """

        # Determine number of keep_alives to send between session refreshes.
        session_refresh_interval_ms = self._bridge.auth_controller.session_refresh_interval_ms
        session_refresh_interval = max(
            int(session_refresh_interval_ms / (KEEP_ALIVE_SIGNAL_INTERVAL_S * 1000)),
            DEFAULT_SIGNALS_PER_SESSION_REFRESH,
        )

        log.info(
            "[KEEP ALIVE] Session refresh interval: %s ms / %s pings",
            session_refresh_interval_ms,
            session_refresh_interval,
        )

        signals_sent = 0
        status_log_counter = 0  # Log status every 5 minutes (5 iterations of 60s)

        while True:
            await asyncio.sleep(KEEP_ALIVE_SIGNAL_INTERVAL_S)

            # Don't send requests if websocket client is disconnected.
            if self.state != WebSocketState.CONNECTED:
                log.debug("[KEEP ALIVE] Skipping keep alive.")
                signals_sent = 0
                status_log_counter = 0
                self._keepalive_failures = 0  # Reset failures when disconnected
                continue

            # Log periodic status every 5 minutes for debugging
            status_log_counter += 1
            if status_log_counter >= 5:
                status_log_counter = 0
                seconds_since_msg = (
                    time.time() - self._last_message_time if self._last_message_time else None
                )
                seconds_connected = (
                    time.time() - self._connected_at if self._connected_at else None
                )
                log.info(
                    "[KEEP ALIVE] Status: state=%s, connected_for=%.0fs, last_msg=%.0fs ago, healthy=%s",
                    self._state.name,
                    seconds_connected or 0,
                    seconds_since_msg or 0,
                    self.is_healthy(),
                )

            # Check connection health and force reconnect if unhealthy
            if not self.is_healthy():
                log.warning(
                    "[KEEP ALIVE] Connection unhealthy (no messages for >5 minutes). Forcing reconnection."
                )
                self._force_reconnect = True
                await self._force_close_websocket()
                continue

            log.debug("[KEEP ALIVE] Sending keep alive.")

            try:
                async with asyncio.timeout(30):  # 30-second timeout for keep-alive operations
                    if signals_sent >= session_refresh_interval - 1:
                        signals_sent = 0
                        await self._reload_session_context()
                    if self._bridge.auth_controller.enable_keep_alive and not await self._bridge.is_logged_in():
                        log.warning("[KEEP ALIVE] Detected expired user session - triggering reconnection")
                        self._force_reconnect = True
                # Reset failure count on success
                self._keepalive_failures = 0
            except SessionExpired:
                log.warning("[KEEP ALIVE] Session expired - triggering reconnection")
                self._force_reconnect = True
            except AuthenticationFailed:
                log.warning("[KEEP ALIVE] Authentication failed - triggering re-auth event")
                self._emit_ws_state(WebSocketState.AUTH_REQUIRED, None)
            except Exception:
                self._keepalive_failures += 1
                log.warning(
                    "[KEEP ALIVE] Error while sending keep alive (failure %s of %s): %s",
                    self._keepalive_failures,
                    MAX_KEEPALIVE_FAILURES,
                    exc_info=True,
                )
                if self._keepalive_failures >= MAX_KEEPALIVE_FAILURES:
                    log.error(
                        "[KEEP ALIVE] Multiple keepalive failures (%s) - triggering reconnection",
                        self._keepalive_failures,
                    )
                    self._force_reconnect = True
                    self._keepalive_failures = 0  # Reset after triggering reconnect

            signals_sent += 1

    #####################
    # REQUEST FUNCTIONS #
    #####################

    async def _authenticate(self) -> None:
        """Get authentication token for websocket endpoint."""

        log.info("Getting WebSocket token.")

        self._token = None

        try:
            response = await self._bridge.get(path="websockets/token", id=None, mini_response=True)
        except AuthenticationFailed:
            # _bridge.get autorepairs when logged out, so we should re-raise AuthenticationFailed.
            log.debug("Primary session expired. Bailing on getting new token.")
            raise
        except (ServiceUnavailable, UnexpectedResponse):
            log.debug("Failed to connect to Alarm.com when authenticating. Try again later.")
            return

        try:
            self._ws_endpoint = response.metadata["endpoint"]
        except KeyError as err:
            raise UnexpectedResponse("Failed to get WebSocket endpoint.") from err

        # Set token only after we have a valid websocket endpoint.
        self._token = response.value

    async def _reload_session_context(self) -> None:
        """Check if we are still logged in."""

        log.info("Reloading session context.")

        url = f"{API_URL_BASE}identities/{self._bridge.auth_controller.profile_id}/reloadContext"
        payload = {"included": [], "meta": {"transformer_version": "1.1"}}

        async with self._bridge.create_request("post", url, json=payload, raise_for_status=True) as rsp:
            text_rsp = await rsp.text()

            if rsp.status >= 400:
                raise UnexpectedResponse(f"Failed to reload session context. Response: {text_rsp}")

        log.debug("Reloaded context. Fetching new token...")

        await self._authenticate()

    def _set_state(self, state: WebSocketState, reconnect_wait: int | None = None) -> None:
        """Set WS client state and emit message only if state has changed."""

        async def emit_state_after_delay(delay: int) -> None:
            """Non-blocking function that emits state after a delay."""
            await asyncio.sleep(delay)
            if self._state == WebSocketState.CONNECTED:
                self._emit_ws_state(WebSocketState.RECONNECTED, None)
            else:
                log.debug("Skipping reconnect emit. No longer connected.")

        if self._state != state:
            old_state = self._state
            self._state = WebSocketState.CONNECTED if state == WebSocketState.RECONNECTED else state

            # Log all state transitions at INFO level for easy debugging
            log.info(
                "[STATE] WebSocket state transition: %s -> %s (reconnect_wait=%s)",
                old_state.name if old_state else "None",
                state.name,
                reconnect_wait,
            )

            if state == WebSocketState.RECONNECTED:
                # Only emit reconnects after 5 second delay. This prevents downstream reconnect events from
                # triggering if the Alarm.com server connects, then immediately drops the connection.
                task = asyncio.create_task(emit_state_after_delay(5))
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)
            else:
                self._emit_ws_state(state, reconnect_wait)

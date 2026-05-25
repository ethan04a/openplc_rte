"""Thin wrapper around paho-mqtt for subscribing to messages."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

import paho.mqtt.client as mqtt

from webserver.logger import get_logger

logger, _ = get_logger("mqtt_sub", use_buffer=False)

MessageHandler = Callable[[str, bytes], None]


def _mqtt_rc_ok(rc: mqtt.MQTTErrorCode | int) -> bool:
    return int(rc) == int(mqtt.MQTT_ERR_SUCCESS)


class MqttSubscriber:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 1883,
        client_id: str | None = None,
        username: str | None = None,
        password: str | None = None,
        keepalive: int = 60,
        tls: bool = False,
        on_message: MessageHandler | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._keepalive = keepalive
        self._username = username
        self._password = password
        self._tls = tls
        self._connected = False
        self._lock = threading.Lock()
        self._handler_lock = threading.Lock()
        self._connect_event = threading.Event()
        self._connect_error: BaseException | None = None
        self._message_handler: MessageHandler | None = on_message

        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
        )
        if username is not None:
            self._client.username_pw_set(username, password or "")
        if tls:
            self._client.tls_set()
        self._client.on_connect = self._on_connect
        self._client.on_connect_fail = self._on_connect_fail
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

    def set_message_handler(self, handler: MessageHandler) -> None:
        with self._handler_lock:
            self._message_handler = handler

    @property
    def is_connected(self) -> bool:
        with self._lock:
            return self._connected

    def connect(self, timeout: float = 5.0) -> None:
        """Connect to the broker and start the background network loop."""
        with self._lock:
            if self._connected:
                return

        self._cleanup_session()
        self._connect_error = None
        self._connect_event.clear()

        try:
            self._client.connect(self._host, self._port, self._keepalive)
            loop_rc = self._client.loop_start()
            if not _mqtt_rc_ok(loop_rc):
                raise RuntimeError(f"MQTT subscriber loop_start failed (rc={loop_rc})")
        except Exception as exc:
            self._cleanup_session()
            raise ConnectionError(
                f"MQTT subscriber failed to connect to {self._host}:{self._port}"
            ) from exc

        if not self._connect_event.wait(timeout):
            self._cleanup_session()
            raise TimeoutError(
                f"MQTT subscriber connect timed out after {timeout}s "
                f"({self._host}:{self._port})"
            )

        if self._connect_error is not None:
            err = self._connect_error
            self._connect_error = None
            raise err

        if not self.is_connected:
            raise ConnectionError(
                f"MQTT subscriber connect finished without a session "
                f"({self._host}:{self._port})"
            )

    def disconnect(self) -> None:
        """Stop the network loop and disconnect from the broker."""
        self._cleanup_session()

    def subscribe(self, topic: str, qos: int = 0) -> bool:
        """Subscribe to a single topic. Returns False if not connected."""
        if not self.is_connected:
            logger.warning("MQTT subscriber not connected; cannot subscribe to %s", topic)
            return False
        result, _mid = self._client.subscribe(topic, qos=qos)
        if not _mqtt_rc_ok(result):
            logger.error("MQTT subscribe failed (rc=%s) topic=%s", result, topic)
            return False
        return True

    def subscribe_many(self, topics: list[tuple[str, int]]) -> bool:
        """Subscribe to multiple topics, e.g. [("a/#", 1), ("b", 0)]."""
        if not topics:
            logger.warning("MQTT subscribe_many called with empty topic list")
            return False
        if not self.is_connected:
            logger.warning("MQTT subscriber not connected; cannot subscribe")
            return False
        result, _mid = self._client.subscribe(topics)
        if not _mqtt_rc_ok(result):
            logger.error("MQTT subscribe_many failed (rc=%s)", result)
            return False
        return True

    def _cleanup_session(self) -> None:
        """Stop loop thread and close broker connection (safe if already idle)."""
        try:
            self._client.loop_stop()
        except (OSError, ValueError) as exc:
            logger.debug("MQTT subscriber loop_stop: %s", exc)
        try:
            self._client.disconnect()
        except OSError as exc:
            logger.debug("MQTT subscriber disconnect: %s", exc)
        with self._lock:
            self._connected = False

    def _finish_connect_attempt(self, error: BaseException | None) -> None:
        self._connect_error = error
        self._connect_event.set()

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del client, userdata, flags, properties
        if reason_code.is_failure:
            logger.error("MQTT subscriber connect failed: %s", reason_code)
            self._finish_connect_attempt(
                ConnectionError(f"MQTT subscriber connect rejected: {reason_code}")
            )
            return
        with self._lock:
            self._connected = True
        self._finish_connect_attempt(None)
        logger.info("MQTT subscriber connected to %s:%s", self._host, self._port)

    def _on_connect_fail(
        self,
        client: mqtt.Client,
        userdata: Any,
    ) -> None:
        del client, userdata
        logger.error("MQTT subscriber connect failed (network)")
        self._finish_connect_attempt(
            ConnectionError(
                f"MQTT subscriber could not reach broker at {self._host}:{self._port}"
            )
        )

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        del client, userdata, flags, properties
        with self._lock:
            self._connected = False
        if reason_code.is_failure:
            logger.warning("MQTT subscriber disconnected: %s", reason_code)
        else:
            logger.info("MQTT subscriber disconnected")

    def _on_message(
        self,
        client: mqtt.Client,
        userdata: Any,
        msg: mqtt.MQTTMessage,
    ) -> None:
        del client, userdata
        with self._handler_lock:
            handler = self._message_handler
        if handler is None:
            return
        topic = msg.topic
        if topic is None:
            logger.warning("MQTT message with undecodable topic; ignored")
            return
        try:
            handler(topic, msg.payload)
        except Exception as exc:
            logger.error("MQTT message handler error on %s: %s", topic, exc)

"""Thin wrapper around paho-mqtt for publishing messages."""

from __future__ import annotations

import json
import threading
from typing import Any

import paho.mqtt.client as mqtt

from webserver.logger import get_logger

logger, _ = get_logger("mqtt_pub", use_buffer=False)


def _mqtt_rc_ok(rc: mqtt.MQTTErrorCode | int) -> bool:
    return int(rc) == int(mqtt.MQTT_ERR_SUCCESS)


class MqttPublisher:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 1883,
        client_id: str | None = None,
        username: str | None = None,
        password: str | None = None,
        keepalive: int = 60,
        tls: bool = False,
    ) -> None:
        self._host = host
        self._port = port
        self._keepalive = keepalive
        self._username = username
        self._password = password
        self._tls = tls
        self._connected = False
        self._lock = threading.Lock()
        self._connect_event = threading.Event()
        self._connect_error: BaseException | None = None

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
                raise RuntimeError(f"MQTT publisher loop_start failed (rc={loop_rc})")
        except Exception as exc:
            self._cleanup_session()
            raise ConnectionError(
                f"MQTT publisher failed to connect to {self._host}:{self._port}"
            ) from exc

        if not self._connect_event.wait(timeout):
            self._cleanup_session()
            raise TimeoutError(
                f"MQTT publisher connect timed out after {timeout}s "
                f"({self._host}:{self._port})"
            )

        if self._connect_error is not None:
            err = self._connect_error
            self._connect_error = None
            raise err

        if not self.is_connected:
            raise ConnectionError(
                f"MQTT publisher connect finished without a session "
                f"({self._host}:{self._port})"
            )

    def disconnect(self) -> None:
        """Stop the network loop and disconnect from the broker."""
        self._cleanup_session()

    def publish(
        self,
        topic: str,
        payload: str | bytes | dict[str, Any],
        qos: int = 0,
        retain: bool = False,
    ) -> bool:
        """
        Publish a message. Dict payloads are serialized as JSON (UTF-8).

        Returns True if the message was queued successfully, False otherwise.
        """
        if not self.is_connected:
            logger.warning("MQTT publisher not connected; drop publish to %s", topic)
            return False

        body: str | bytes
        if isinstance(payload, dict):
            body = json.dumps(payload, ensure_ascii=False)
        else:
            body = payload

        info = self._client.publish(topic, body, qos=qos, retain=retain)
        if not _mqtt_rc_ok(info.rc):
            logger.error("MQTT publish failed (rc=%s) topic=%s", info.rc, topic)
            return False
        return True

    def _cleanup_session(self) -> None:
        try:
            self._client.loop_stop()
        except (OSError, ValueError) as exc:
            logger.debug("MQTT publisher loop_stop: %s", exc)
        try:
            self._client.disconnect()
        except OSError as exc:
            logger.debug("MQTT publisher disconnect: %s", exc)
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
            logger.error("MQTT publisher connect failed: %s", reason_code)
            self._finish_connect_attempt(
                ConnectionError(f"MQTT publisher connect rejected: {reason_code}")
            )
            return
        with self._lock:
            self._connected = True
        self._finish_connect_attempt(None)
        logger.info("MQTT publisher connected to %s:%s", self._host, self._port)

    def _on_connect_fail(
        self,
        client: mqtt.Client,
        userdata: Any,
    ) -> None:
        del client, userdata
        logger.error("MQTT publisher connect failed (network)")
        self._finish_connect_attempt(
            ConnectionError(
                f"MQTT publisher could not reach broker at {self._host}:{self._port}"
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
            logger.warning("MQTT publisher disconnected: %s", reason_code)
        else:
            logger.info("MQTT publisher disconnected")

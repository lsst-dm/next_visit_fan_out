# This file is part of next_visit_fan_out.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import asyncio
import collections.abc
import dataclasses
import datetime
import json
import logging
import os
from pathlib import Path
import ssl
import sys
import time
import typing

from aiokafka import AIOKafkaConsumer  # type:ignore
import httpx
from kafkit.registry import Deserializer
from kafkit.registry.httpx import RegistryApi
import redis.asyncio as redis
from redis.backoff import NoBackoff
from redis.exceptions import (ConnectionError, TimeoutError, WatchError)
from redis.asyncio.retry import Retry
import yaml

from shared.visit import NextVisitModelBase, NextVisitModelKeda


@dataclasses.dataclass(frozen=True)
class InstrumentConfig:
    """The configuration used for sending messages to a specific instrument.

    Parameters
    ----------
    conf : `dict`
        A hierarchical instrument configuration, whose keys are instruments.
    instrument : `str`
        The instrument to configure.
    """

    instrument: str
    """The instrument being configured."""
    stream: str
    """The name of the redis stream for this instrument (`str`)."""
    detectors: collections.abc.Sequence[int]
    """The active detectors for this instrument (sequence [`int`])."""

    def __init__(self, conf, instrument):
        super().__setattr__("instrument", instrument)
        super().__setattr__("stream", conf["redis-streams"][instrument])
        super().__setattr__("detectors", self.detector_load(conf, instrument))

    @staticmethod
    def detector_load(conf: dict, instrument: str) -> list[int]:
        """Load active instrument detectors from yaml configiration file of
        true false values for each detector.

        Parameters
        ----------
        conf : `dict`
            The instrument configuration from the yaml file.
        instrument : `str`
            The instrument to load detectors for.

        Returns
        -------
        active_detectors : `list` [`int`]
            The active detectors for the instrument.
        """
        detectors = conf["detectors"][instrument]
        active_detectors: list[int] = []
        for detector, active in detectors.items():
            if active:
                active_detectors.append(int(detector))
        return active_detectors


@dataclasses.dataclass(frozen=True)
class Submission:
    """The batched requests to be submitted.
    """

    stream: str
    """The redis stream (`str`)."""
    fan_out_messages: collections.abc.Collection[dict[str, typing.Any]]
    """The messages to send (collection [`dict`])."""


class UnsupportedMessageError(RuntimeError):
    """Exception raised if there is no Prompt Processing instance for a given
    nextVisit message.
    """
    pass


class UsdfJsonFormatter(logging.Formatter):
    """A formatter that can be parsed by the Loki/Grafana system at USDF.

    The formatter's output is a JSON-encoded message with "flattened" metadata
    to make it easy to inspect with Grafana filters.
    """
    def format(self, record):
        # format updates record.message, but the full info is *only* in the
        # return value.
        msg = super().format(record)

        # Many LogRecord attributes are only useful for interrogating the
        # record in Python; filter to what's useful in Grafana.
        entry = {
            # formatTime only automatically handles msecs (uuu) with the
            # default format, and the assumption that they're the last part of
            # the string is hardcoded. Use manual formatting instead.
            # RFC3339Nano is the only buit-in promtail format that supports
            # fractional seconds.
            "asctime": self.formatTime(record, datefmt='%Y-%m-%dT%H:%M:%S.%(msecs)03d%z')
            % {"msecs": record.msecs},
            "funcName": record.funcName,
            "level": record.levelname,  # "level" auto-parsed by Grafana
            "levelno": record.levelno,
            "lineno": record.lineno,
            "message": msg,
            "name": record.name,
            "pathname": record.pathname,
            "process": record.process,
            "thread": record.thread,
        }

        return json.dumps(entry)


def is_handleable(message: dict[str, typing.Any],
                  expire: float,
                  active_instruments: collections.abc.Collection[str]) -> bool:
    """Test whether a nextVisit message should be handled by fan-out.

    This function emits explanatory logs as a side effect.

    Parameters
    ----------
    message : `dict` [`str`]
        An unpacked mapping of message fields.
    expire : `float`
        The maximum age, in seconds, that a message can still be handled.
    active_instruments : collection [`str`]
        The set of instruments whose messages should be handled.

    Returns
    -------
    handleable : `bool`
        `True` is the message can be processed, `False` otherwise.
    """
    if not message["instrument"]:
        logging.info("Message does not have an instrument. Assuming it's not an observation.")
        return False
    if message["instrument"] not in active_instruments:
        logging.info(f"Instrument {message['instrument']} is not active, ignoring.")
        return False

    # efdStamp is visit publication, in seconds since 1970-01-01 UTC
    if message["private_efdStamp"]:
        published = message["private_efdStamp"]
        age = round(time.time() - published)  # Microsecond precision is distracting
        if age > expire:
            logging.warning("Message published on %s UTC is %s old, ignoring.",
                            time.ctime(published),
                            datetime.timedelta(seconds=age)
                            )
            return False
    else:
        logging.warning("Message does not have private_efdStamp, can't determine age.")
    return True


def make_fanned_out_messages(
    message: NextVisitModelBase,
    instruments: collections.abc.Mapping[str, InstrumentConfig],
    upload_test_detectors: collections.abc.Mapping[int, collections.abc.Collection[int]],
) -> Submission:
    """Create appropriate fanned-out messages for an incoming message.

    Parameters
    ----------
    message : `NextVisitModelBase`
        The message to fan out.
    instruments : mapping [`str`, `InstrumentConfig`]
        A mapping from instrument name to configuration information.
    upload_test_detectors : mapping [`int`, collection [`int`]]
        A mapping from visit to the supported detectors for that visit.
        This is used by upload.py test where a smaller dataset is uploaded
        for HSC and LSSTCam.

    Returns
    -------
    send_info : `Submission`
        The fanned out messages, along with where to send them.

    Raises
    ------
    UnsupportedMessageError
        Raised if ``message`` cannot be fanned-out or sent.
    """

    match (message.instrument, message.salIndex):
        case ("HSC", 999):
            # Datasets from using upload_from_repo.py
            return fan_out(message, instruments[message.instrument])
        case ("HSC" | "LSSTCam", visit) if visit in upload_test_detectors:
            # HSC and LSSTCam have extra active detector
            # configurations just for the upload.py test datasets.
            return fan_out_upload_test(
                message,
                instruments[message.instrument],
                upload_test_detectors[visit],
            )
        case (instrument, _) if instrument in instruments:
            return fan_out(message, instruments[instrument])
        case _:
            raise UnsupportedMessageError(
                f"No matching case for instrument {message.instrument} salIndex {message.salIndex}"
            )


def fan_out(next_visit, inst_config):
    """Prepare fanned-out messages for sending to the Prompt Processing
    service.

    Parameters
    ----------
    next_visit : `NextVisitModelBase`
        The nextVisit message to fan out.
    inst_config : `InstrumentConfig`
        The configuration information for the active instrument.

    Returns
    -------
    fanned_out : `Submission`
        The submission information for the fanned-out messages.
    """
    return Submission(inst_config.stream, next_visit.add_detectors(inst_config.detectors))


def fan_out_upload_test(next_visit, inst_config, detectors):
    """Prepare fanned-out messages for upload.py test with selected detectors.

    Parameters
    ----------
    next_visit : `NextVisitModelBase`
        The nextVisit message to fan out.
    inst_config : `InstrumentConfig`
        The configuration information for the active instrument.
    detectors : collection [`int`]
        The detectors to send.

    Returns
    -------
    fanned_out : `Submission`
        The submission information for the fanned-out messages.
    """
    return Submission(inst_config.stream, next_visit.add_detectors(detectors))


async def redis_stream_request(
    redis_client: redis.Redis,
    send_info: Submission,
    marker: str,
    max_attempts: int,
    base_delay: float,
    max_delay: float,
) -> None:
    """Makes a redis stream request, re-driving across transient failures.

    Each visit is sent with an application-level bounded retry loop so that
    a brief Redis outage (e.g. a restart) does not drop it. Redis retry is
    disabled. Messages are all pipelined in a transaction along with a nonce
    to avoid resubmission of a visit on certain errors.

    Parameters
    ----------
    redis_client : `redis.Redis`
        The redis stream client.
    send_info : `Submission`
        The data and address to submit.
    marker : `str`
        A nonce to avoid resubmitting a visit after a network fault.
    max_attempts : `int`
        Maximum number of application-level send attempts.
    base_delay : `float`
        Initial backoff delay (seconds).
    max_delay : `float`
        Maximum backoff delay (seconds).
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            async with redis_client.pipeline() as pipe:
                while True:
                    try:
                        await pipe.watch(marker)
                        if await pipe.exists(marker):
                            logging.info("Visit already sent [%s]", marker)
                            return
                        pipe.multi()
                        pipe.set(marker, 1, ex=360)
                        for body in send_info.fan_out_messages:
                            pipe.xadd(send_info.stream, body)
                        await pipe.execute()
                        logging.info("Sent %d messages for [%s]",
                                     len(send_info.fan_out_messages),
                                     marker)
                        return
                    except WatchError:
                        logging.warning("Redis WATCH failed while sending [%s], checking again",
                                        marker)
                        continue
        except (ConnectionError, TimeoutError) as e:
            if attempt >= max_attempts:
                logging.error(
                    "Giving up on [%s] after %d attempts: %s",
                    marker, attempt, e,
                )
                # Let main loop figure it out.
                raise
            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            logging.warning(
                "batch xadd for visit [%s] failed (attempt %d/%d), retrying in "
                "%.1fs: %s",
                marker, attempt, max_attempts, delay, e,
            )
            await asyncio.sleep(delay)
        except Exception:
            # Unexpected error: log with traceback and stop retrying so we do
            # not loop forever on a non-transient problem.
            logging.exception(
                "Unexpected error, dropping visit [%s]", marker
            )
            return


async def main() -> None:
    # Get environment variables
    supported_instruments = os.environ["SUPPORTED_INSTRUMENTS"].split()
    instrument_config_file = os.environ["INSTRUMENT_CONFIG_FILE"]
    kafka_cluster = os.environ["KAFKA_CLUSTER"]
    group_id = os.environ["CONSUMER_GROUP"]
    topic = os.environ["NEXT_VISIT_TOPIC"]
    offset = os.environ["OFFSET"]
    expire = float(os.environ["MESSAGE_EXPIRATION"])
    kafka_schema_registry_url = os.environ["KAFKA_SCHEMA_REGISTRY_URL"]
    max_outgoing = int(os.environ["MAX_FAN_OUT_MESSAGES"])
    # Platform that prompt processing will run on
    platform = os.environ["PLATFORM"].lower()
    # Redis Stream cluster
    redis_host = os.environ["REDIS_HOST"]
    # Redis health check interval
    redis_health_check_interval = int(os.environ.get("REDIS_HEALTH_CHECK_INTERVAL", 3))
    # Redis socket timeouts (seconds) so sends/retries fail fast on a dead or
    # slow network and release their connections instead of hanging.
    redis_connect_timeout = float(os.environ.get("REDIS_CONNECT_TIMEOUT", 5))
    redis_socket_timeout = float(os.environ.get("REDIS_SOCKET_TIMEOUT", 5))
    # Redis connection pool size. Leave substantial headroom (~ 100) as it is
    # harmless. Normal operation uses just one or two connections.
    redis_max_connections = int(os.environ.get("REDIS_MAX_CONNECTIONS", 100))
    # Application-level re-drive parameters (span a Redis restart window).
    redis_send_attempts = int(os.environ.get("REDIS_SEND_ATTEMPTS", 5))
    redis_send_base_delay = float(os.environ.get("REDIS_SEND_BASE_DELAY", 1))
    redis_send_max_delay = float(os.environ.get("REDIS_SEND_MAX_DELAY", 10))

    # kafka auth
    sasl_username = os.environ["SASL_USERNAME"]
    sasl_password = os.environ["SASL_PASSWORD"]
    sasl_mechanism = os.environ["SASL_MECHANISM"]
    security_protocol = os.environ["SECURITY_PROTOCOL"]

    # Logging config
    log_handler = logging.StreamHandler(stream=sys.stdout)
    log_handler.setFormatter(UsdfJsonFormatter())
    if os.environ.get("DEBUG_LOGS") == "true":
        logging.basicConfig(handlers=[log_handler], level=logging.DEBUG)
    else:
        logging.basicConfig(handlers=[log_handler], level=logging.INFO)

    # Note: if we start supporting multiple platforms again, filter here so
    # the loop doesn't have to raise an error.
    if platform != "keda":
        raise ValueError(f"no valid platform defined, got '{platform}'")

    conf = yaml.safe_load(Path(instrument_config_file).read_text())
    known_instruments = {inst for inst in conf["detectors"].keys() if "-TEST-" not in inst}
    logging.debug("Known instruments: %s", known_instruments)
    instruments = {inst: InstrumentConfig(conf, inst) for inst in known_instruments}
    # These groups are for the small datasets used in the upload.py test
    upload_test_detectors = {
        visit: InstrumentConfig.detector_load(conf, f"HSC-TEST-{visit}")
        for visit in {59134, 59142, 59150, 59160}
    } | {
        visit: InstrumentConfig.detector_load(conf, f"LSSTCam-TEST-{visit}")
        for visit in {2026010100039, 2026010100047, 2026010100048, 2026010100050, 2026010100267}
    }

    # Create ssl context for Kafka consumer
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False  # TODO set to true when migrated to prompt-kafka
    ssl_context.verify_mode = ssl.CERT_NONE  # TODO enable when migrated to prompt-kafka

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=kafka_cluster,
        group_id=group_id,
        auto_offset_reset=offset,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=sasl_username,
        sasl_plain_password=sasl_password,
        ssl_context=ssl_context,
        enable_auto_commit=False,
    )

    await consumer.start()

    # Define before the try so cleanup in the finally block is always safe.
    redis_client = None

    limits = httpx.Limits(max_connections=max_outgoing)
    async with httpx.AsyncClient(limits=limits) as client:

        try:
            # Setup kafka schema registry connection and deserialzer
            registry_api = RegistryApi(
                http_client=client, url=kafka_schema_registry_url
            )
            deserializer = Deserializer(registry=registry_api)

            # Redis client setup with retry and exponential backoff
            # https://redis.io/kb/doc/22wxq63j93/how-to-manage-client-reconnections-in-case-of-errors-with-redis-py
            # Finite socket timeouts ensure sends/retries fail fast on a dead
            # or slow network and release connections instead of hanging and
            # draining the pool (root cause of MaxConnectionsError).
            # The client lives for the whole run and is closed in the finally
            # block below, after in-flight tasks are drained.
            redis_client = redis.Redis(
                host=redis_host,
                max_connections=redis_max_connections,
                socket_connect_timeout=redis_connect_timeout,
                socket_timeout=redis_socket_timeout,
                health_check_interval=redis_health_check_interval,
                retry=Retry(NoBackoff(), 0),
            )

            while True:  # run continously
                async for msg in consumer:
                    try:
                        next_visit_message_initial = await deserializer.deserialize(
                            data=msg.value
                        )
                        logging.info(f"message deserialized: offset[{msg.offset}] "
                                     f"timestamp[{msg.timestamp}] {next_visit_message_initial}")
                        if not is_handleable(next_visit_message_initial["message"],
                                             expire,
                                             supported_instruments):
                            continue

                        next_visit_message_updated = NextVisitModelKeda.from_raw_message(
                            next_visit_message_initial["message"]
                        )
                        send_info = make_fanned_out_messages(next_visit_message_updated,
                                                             instruments,
                                                             upload_test_detectors)
                        marker = f"{send_info.stream}:sent:{msg.topic}:{msg.partition}:{msg.offset}"
                        await redis_stream_request(redis_client, send_info,
                                                   marker, redis_send_attempts,
                                                   redis_send_base_delay,
                                                   redis_send_max_delay)
                    except (ConnectionError, TimeoutError):
                        logging.exception("Could not submit visit to Redis, skipping.")
                    except Exception:
                        # TODO: Find more exceptions which we can handle as
                        # temporary. Schema registry outage? What actually
                        # breaks temporarily, and not for very long?
                        logging.exception("Could not process message, skipping.")
                    # Commit here and not in a finally so deliberately
                    # stopping the process does not commit unsent visits.
                    await consumer.commit()

        finally:
            # Stop consuming new Kafka messages first.
            await consumer.stop()
            # Close the Redis client once, on shutdown (not after construction)
            if redis_client is not None:
                await redis_client.aclose()


asyncio.run(main())

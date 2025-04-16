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
from cloudevents.conversion import to_structured
from cloudevents.http import CloudEvent
import httpx
from kafkit.registry import Deserializer
from kafkit.registry.httpx import RegistryApi
from prometheus_client import start_http_server, Summary  # type:ignore
from prometheus_client import Gauge
import redis.asyncio as redis
from redis.backoff import ExponentialBackoff
from redis.exceptions import (ConnectionError, TimeoutError)
from redis.retry import Retry
import yaml

from shared.visit import NextVisitModelKeda, NextVisitModelKnative

REQUEST_TIME = Summary("request_processing_seconds", "Time spent processing request")


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
    """The instrument whose metrics are held by this object (`str`)."""
    url: str
    """The address of the Knative Serving instance for this instrument (`str`)."""
    stream: str
    """The name of the redis stream for this instrument (`str`)."""
    detectors: collections.abc.Sequence[int]
    """The active detectors for this instrument (sequence [`int`])."""

    def __init__(self, conf, instrument):
        super().__setattr__("instrument", instrument)
        super().__setattr__("url", conf["knative-urls"][instrument])
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
class Metrics:
    """A container for all metrics associated with a specific instrument.

    Parameters
    ----------
    instrument : `str`
        The instrument whose metrics are held by this object.
    """

    instrument: str
    """The instrument whose metrics are held by this object (`str`)."""
    total_received: Gauge
    """The number of incoming messages processed by this instance (`prometheus_client.Gauge`)."""
    in_process: Gauge
    """The number of fanned-out messages currently being processed (`prometheus_client.Gauge`)."""

    def __init__(self, instrument):
        super().__setattr__("instrument", instrument)
        word_instrument = instrument.lower().replace(" ", "_").replace("-", "_")
        super().__setattr__("total_received",
                            Gauge(word_instrument + "_next_visit_messages",
                                  f"next visit messages with {instrument} as instrument"))
        super().__setattr__("in_process",
                            Gauge(word_instrument + "_prompt_processing_in_process_requests",
                                  f"{instrument} in process requests for next visit"))


@dataclasses.dataclass(frozen=True)
class Submission:
    """The batched requests to be submitted to a Knative instance.
    """

    url: str
    """The address of the Knative Serving instance to send requests to (`str`)."""
    stream: str
    """The redis stream (`str`)."""
    fan_out_messages: collections.abc.Collection[dict[str, typing.Any]]
    """The messages to send to ``url`` (collection [`dict`])."""


class UnsupportedMessageError(RuntimeError):
    """Exception raised if there is no Prompt Processing instance for a given
    nextVisit message.
    """
    pass


def is_handleable(message: dict[str, typing.Any], expire: float) -> bool:
    """Test whether a nextVisit message has enough data to be handled by
    fan-out.

    This function emits explanatory logs as a side effect.

    Parameters
    ----------
    message : `dict` [`str`]
        An unpacked mapping of message fields.
    expire : `float`
        The maximum age, in seconds, that a message can still be handled.

    Returns
    -------
    handleable : `bool`
        `True` is the message can be processed, `False` otherwise.
    """
    if not message["instrument"]:
        logging.info("Message does not have an instrument. Assuming it's not an observation.")
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


def make_fanned_out_messages_keda(
    message: NextVisitModelKeda,
    instruments: collections.abc.Mapping[str, InstrumentConfig],
    upload_test_detectors: collections.abc.Mapping[int, collections.abc.Collection[int]],
) -> Submission:
    """Create appropriate fanned-out messages for an incoming message for Keda.

    Parameters
    ----------
    message : `NextVisitModel`
        The message to fan out.
    instruments : mapping [`str`, `InstrumentConfig`]
        A mapping from instrument name to configuration information.
    gauges : mapping [`str`, `Metrics`]
        A mapping from instrument name to metrics for that instrument.
    upload_test_detectors : mapping [`int`, collection [`int`]]
        A mapping from visit to the supported detectors for that visit.
        This is used by upload.py test where a smaller dataset is uploaded
        for HSC and LSSTCam-imSim.

    Returns
    -------
    send_info : `Submission`
        The fanned out messages, along with where to send them.

    Raises
    ------
    UnsupportedMessageError
        Raised if ``message`` cannot be fanned-out or sent.
    """
    match message.instrument:
        case "HSC" | "LSSTCam-imSim":
            # HSC and LSSTCam-imSim have extra active detector configurations just
            # for the upload.py test.
            match message.salIndex:
                case 999:  # Datasets from using upload_from_repo.py
                    return fan_out(message, instruments[message.instrument])
                case visit if visit in upload_test_detectors:  # upload.py test datasets
                    return fan_out_upload_test(
                        message,
                        instruments[message.instrument],
                        upload_test_detectors[visit],
                    )
                case _:
                    raise UnsupportedMessageError(
                        f"No matching case for {message.instrument} salIndex {message.salIndex}"
                    )
        case instrument if instrument in instruments:
            return fan_out(message, instruments[instrument])
        case _:
            raise UnsupportedMessageError(f"no matching case for instrument {message.instrument}.")


def make_fanned_out_messages_knative(
    message: NextVisitModelKnative,
    instruments: collections.abc.Mapping[str, InstrumentConfig],
    gauges: collections.abc.Mapping[str, Metrics],
    upload_test_detectors: collections.abc.Mapping[int, collections.abc.Collection[int]],
) -> Submission:
    """Create appropriate fanned-out messages for an incoming message.

    Parameters
    ----------
    message : `NextVisitModel`
        The message to fan out.
    instruments : mapping [`str`, `InstrumentConfig`]
        A mapping from instrument name to configuration information.
    gauges : mapping [`str`, `Metrics`]
        A mapping from instrument name to metrics for that instrument.
    upload_test_detectors : mapping [`int`, collection [`int`]]
        A mapping from visit to the supported detectors for that visit.
        This is used by upload.py test where a smaller dataset is uploaded
        for HSC and LSSTCam-imSim.

    Returns
    -------
    send_info : `Submission`
        The fanned out messages, along with where to send them.

    Raises
    ------
    UnsupportedMessageError
        Raised if ``message`` cannot be fanned-out or sent.
    """
    match message.instrument:
        case "HSC" | "LSSTCam-imSim":
            # HSC and LSSTCam-imSim have extra active detector configurations just
            # for the upload.py test.
            match message.salIndex:
                case 999:  # Datasets from using upload_from_repo.py
                    gauges[message.instrument].total_received.inc()
                    return fan_out(message, instruments[message.instrument])
                case visit if visit in upload_test_detectors:  # upload.py test datasets
                    gauges[message.instrument].total_received.inc()
                    return fan_out_upload_test(
                        message,
                        instruments[message.instrument],
                        upload_test_detectors[visit],
                    )
                case _:
                    raise UnsupportedMessageError(
                        f"No matching case for {message.instrument} salIndex {message.salIndex}"
                    )
        case instrument if instrument in instruments:
            gauges[instrument].total_received.inc()
            return fan_out(message, instruments[instrument])
        case _:
            raise UnsupportedMessageError(f"no matching case for instrument {message.instrument}.")


def fan_out(next_visit, inst_config):
    """Prepare fanned-out messages for sending to the Prompt Processing service.

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
    return Submission(inst_config.url, inst_config.stream, next_visit.add_detectors(inst_config.detectors))


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
    return Submission(inst_config.url, inst_config.stream, next_visit.add_detectors(detectors))


def dispatch_fanned_out_messages_knative(client: httpx.AsyncClient,
                                         topic: str,
                                         tasks: collections.abc.MutableSet[asyncio.Task],
                                         send_info: Submission,
                                         gauges: collections.abc.Mapping[str, Metrics],
                                         *,
                                         retry_knative: bool,
                                         ):
    """Package and send the fanned-out messages to Knative Prompt Processing.

    Parameters
    ----------
    client : `httpx.AsyncClient`
        The client to which to upload the messages.
    topic : `str`
        The topic to which to upload the messages.
    tasks : set [`asyncio.Task`]
        Collection for holding the requests.
    send_info : `Submission`
        The data and address to submit.
    gauges : mapping [`str`, `Metrics`]
        A mapping from instrument name to metrics for that instrument.
    retry_knative : `bool`
        Whether or not Knative requests can be retried.
    """
    try:
        attributes = {
            "type": "com.example.kafka",
            "source": topic,
        }

        for fan_out_message in send_info.fan_out_messages:
            data = fan_out_message
            data_json = json.dumps(data)

            logging.info(f"data after json dump {data_json}")
            event = CloudEvent(attributes, data_json)
            headers, body = to_structured(event)
            info = {
                key: data[key] for key in ["instrument", "groupId", "detector"]
            }

            task = asyncio.create_task(
                knative_request(
                    gauges[fan_out_message["instrument"]].in_process,
                    client,
                    send_info.url,
                    headers,
                    body,
                    str(info),
                    retry=retry_knative,
                )
            )
            tasks.add(task)
            task.add_done_callback(tasks.discard)

    except ValueError:
        logging.exception("Error while sending fanned-out messages.")


def dispatch_fanned_out_messages_redis_stream(redis_client: redis.Redis,
                                              tasks: collections.abc.MutableSet[asyncio.Task],
                                              send_info: Submission,
                                              ):
    """Package and send the fanned-out messages to Prompt Processing.

    Parameters
    ----------
    redis_client : `redis.Redis`
        The redis client used to send the redis stream.
    tasks : set [`asyncio.Task`]
        Collection for holding the requests.
    send_info : `Submission`
        The data and address to submit.
    """

    stream = send_info.stream

    try:
        for fan_out_message in send_info.fan_out_messages:

            task = asyncio.create_task(
                redis_stream_request(
                    redis_client,
                    stream,
                    fan_out_message,
                )
            )
            tasks.add(task)
            task.add_done_callback(tasks.discard)

    except ValueError:
        logging.exception("Error while sending fanned-out messages.")


@REQUEST_TIME.time()
async def knative_request(
    in_process_requests_gauge,
    client: httpx.AsyncClient,
    knative_serving_url: str,
    headers: dict[str, str],
    body: bytes,
    info: str,
    *,
    retry: bool,
) -> None:
    """Makes knative http request.

    Parameters
    ----------
    in_process_requests_gauge : `prometheus_client.Gauge`
        A gauge to be updated with the start and end of the request.
    client : `httpx.AsyncClient`
        The async httpx client.
    knative_serving_url : `string`
        The url for the knative instance.
    headers : dict[`str,'str']
        The headers to pass to knative.
    body : `bytes`
        The next visit message body.
    info : `str`
        Information such as some fields of the next visit message to identify
        this request and to log with.
    retry : `bool`
        Whether or not requests can be retried.
    """
    with in_process_requests_gauge.track_inprogress():
        result = await client.post(
            knative_serving_url,
            headers=headers,
            data=body,  # type:ignore
            timeout=None,
        )

        logging.info(
            f"nextVisit {info} status code {result.status_code} for initial request {result.content}"
        )

        if retry and result.status_code == 503:
            if 'Retry-After' in result.headers:
                delay = int(result.headers['Retry-After'])
                logging.info("Waiting %d seconds before retrying nextVisit %s...", delay, info)
                await asyncio.sleep(delay)

            logging.info(
                f"retry after status code {result.status_code} for nextVisit {info}"
            )
            retry_result = await client.post(
                knative_serving_url,
                headers=headers,
                data=body,  # type:ignore
                timeout=None,
            )
            logging.info(
                f"nextVisit {info} status code {retry_result.status_code} for "
                f"retried request {retry_result.content}"
            )


async def redis_stream_request(
    redis_client: redis.Redis,
    redis_stream: str,
    body: dict[str, typing.Any],
) -> None:
    """Makes redis stream request.

    Parameters
    ----------
    redis_client : `redis.Redis`
        The redis stream client.
    redis_stream : `str`
        The name of the redis stream.
    body : `dict` [`str`, `typing.Any`]
        The next visit message body.
    """

    redis_entry_id = await redis_client.xadd(
        redis_stream,
        body
    )
    logging.info(f"Sent msg {body} for redis stream {redis_stream} with id {redis_entry_id}")


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
    retry_knative = os.environ["RETRY_KNATIVE_REQUESTS"].lower() == "true"
    # Platform that prompt processing will run on
    platform = os.environ["PLATFORM"].lower()
    # Redis Stream cluster
    redis_host = os.environ["REDIS_HOST"]
    # Maximum delay time for Redis retries in seconds
    redis_retry_delay_cap = int(os.environ.get("REDIS_RETRY_DELAY_CAP", 5))
    # Initial delay for first Redis retry in seconds
    redis_retry_initial_delay = int(os.environ.get("REDIS_RETRY_INITIAL_DELAY", 1))
    # Redis max retry count
    redis_retry_count = int(os.environ.get("REDIS_RETRY_COUNT", 3))
    # Redis health check interval
    redis_health_check_interval = int(os.environ.get("REDIS_HEALTH_CHECK_INTERVAL", 3))

    # kafka auth
    sasl_username = os.environ["SASL_USERNAME"]
    sasl_password = os.environ["SASL_PASSWORD"]
    sasl_mechanism = os.environ["SASL_MECHANISM"]
    security_protocol = os.environ["SECURITY_PROTOCOL"]

    # Logging config
    if os.environ.get("DEBUG_LOGS") == "true":
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    else:
        logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    conf = yaml.safe_load(Path(instrument_config_file).read_text())
    instruments = {inst: InstrumentConfig(conf, inst) for inst in supported_instruments}
    # These groups are for the small datasets used in the upload.py test
    upload_test_detectors = {
        visit: InstrumentConfig.detector_load(conf, f"HSC-TEST-{visit}")
        for visit in {59134, 59142, 59150, 59160}
    } | {
        visit: InstrumentConfig.detector_load(conf, f"LSSTCam-imSim-TEST-{visit}")
        for visit in {496960, 496989}
    }

    # Start Prometheus endpoint
    start_http_server(8000)

    # Create ssl context for Kafka consumer
    ssl_context = context = ssl.create_default_context()
    ssl_context.check_hostname = False # TODO set to true when migrated to prompt-kafka
    ssl_context.verify_mode=ssl.CERT_NONE # TODO enable when migrated to prompt-kafka

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
    )

    gauges = {inst: Metrics(inst) for inst in supported_instruments}

    await consumer.start()

    tasks = set()

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
            redis_client = redis.Redis(host=redis_host,
                                       retry=Retry(ExponentialBackoff(cap=redis_retry_delay_cap,
                                                                      base=redis_retry_initial_delay),
                                                   redis_retry_count),
                                       retry_on_error=[ConnectionError, TimeoutError, ConnectionResetError],
                                       health_check_interval=redis_health_check_interval)
            await redis_client.aclose()

            while True:  # run continously
                async for msg in consumer:
                    try:
                        next_visit_message_initial = await deserializer.deserialize(
                            data=msg.value
                        )
                        logging.info(f"message offset {msg.offset} and timestamp {msg.timestamp}")
                        logging.info(f"message deserialized {next_visit_message_initial}")
                        if not is_handleable(next_visit_message_initial["message"], expire):
                            continue

                        if platform == "knative":
                            next_visit_message_updated = NextVisitModelKnative.from_raw_message(
                                next_visit_message_initial["message"]
                            )
                            send_info = make_fanned_out_messages_knative(next_visit_message_updated,
                                                                         instruments,
                                                                         gauges,
                                                                         upload_test_detectors)
                            dispatch_fanned_out_messages_knative(client, topic, tasks, send_info, gauges,
                                                                 retry_knative=retry_knative)
                        elif platform == "keda":
                            next_visit_message_updated = NextVisitModelKeda.from_raw_message(
                                next_visit_message_initial["message"]
                            )
                            send_info = make_fanned_out_messages_keda(next_visit_message_updated,
                                                                      instruments,
                                                                      upload_test_detectors)
                            dispatch_fanned_out_messages_redis_stream(redis_client, tasks, send_info)
                        else:
                            raise ValueError("no valid platform defined")
                    except UnsupportedMessageError:
                        logging.exception("Could not process message, continuing.")
        finally:
            await consumer.stop()


asyncio.run(main())

import asyncio
import collections.abc
import dataclasses
import datetime
import json
import logging
import os
from pathlib import Path
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
import yaml

REQUEST_TIME = Summary("request_processing_seconds", "Time spent processing request")


@dataclasses.dataclass
class NextVisitModel:
    "Next Visit Message"
    salIndex: int
    scriptSalIndex: int
    instrument: str
    groupId: str
    coordinateSystem: int
    position: list[float]
    startTime: float
    rotationSystem: int
    cameraAngle: float
    filters: str
    dome: int
    duration: float
    nimages: int
    survey: str
    totalCheckpoints: int
    private_sndStamp: float

    def add_detectors(
        self,
        active_detectors: list,
    ) -> list[dict[str, typing.Any]]:
        """Adds and duplicates this message for fanout.

        Parameters
        ----------
        active_detectors: `list`
            The active detectors for an instrument.

        Returns
        -------
        message_list : `list` [`dict`]
            The message list for fan out.
        """
        message = dataclasses.asdict(self)
        message_list: list[dict[str, typing.Any]] = []
        for active_detector in active_detectors:
            temp_message = message.copy()
            temp_message["detector"] = active_detector
            # temporary change to modify short filter names to format expected by butler
            if temp_message["filters"] != "" and len(temp_message["filters"]) == 1:
                temp_message["filters"] = (
                    "SDSS" + temp_message["filters"] + "_65mm~empty"
                )
            message_list.append(temp_message)
        return message_list


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
    detectors: collections.abc.Sequence[int]
    """The active detectors for this instrument (sequence [`int`])."""

    def __init__(self, conf, instrument):
        super().__setattr__("instrument", instrument)
        super().__setattr__("url", conf["knative-urls"][instrument])
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
        word_instrument = instrument.lower().replace(" ", "_")
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
    fan_out_messages: collections.abc.Collection[dict[str, typing.Any]]
    """The messages to send to ``url`` (collection [`dict`])."""


def fan_out(next_visit, inst_config):
    """Prepare fanned-out messages for sending to the Prompt Processing service.

    Parameters
    ----------
    next_visit : `NextVisitModel`
        The nextVisit message to fan out.
    inst_config : `InstrumentConfig`
        The configuration information for the active instrument.

    Returns
    -------
    fanned_out : `Submission`
        The submission information for the fanned-out messages.
    """
    return Submission(inst_config.url, next_visit.add_detectors(inst_config.detectors))


def fan_out_hsc(next_visit, inst_config, detectors):
    """Prepare fanned-out messages for HSC upload.py.

    Parameters
    ----------
    next_visit : `NextVisitModel`
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
    return Submission(inst_config.url, next_visit.add_detectors(detectors))


@REQUEST_TIME.time()
async def knative_request(
    in_process_requests_gauge,
    client: httpx.AsyncClient,
    knative_serving_url: str,
    headers: dict[str, str],
    body: bytes,
    info: str,
) -> None:
    """Makes knative http request.

    Parameters
    ----------
    client: `httpx.AsyncClient`
        The async httpx client.
    knative_serving_url : `string`
        The url for the knative instance.
    headers: dict[`str,'str']
        The headers to pass to knative.
    body: `bytes`
        The next visit message body.
    info: `str`
        Information such as some fields of the next visit message to identify
        this request and to log with.
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

        '''
        if result.status_code == 502 or result.status_code == 503:
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
                f"nextVisit {info} retried request {retry_result.content}"
            )
        '''


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

    # kafka auth
    sasl_username = os.environ["SASL_USERNAME"]
    sasl_password = os.environ["SASL_PASSWORD"]
    sasl_mechanism = os.environ["SASL_MECHANISM"]
    security_protocol = os.environ["SECURITY_PROTOCOL"]

    # Logging config
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logging.basicConfig(stream=sys.stderr, level=logging.WARNING)

    conf = yaml.safe_load(Path(instrument_config_file).read_text())
    instruments = {inst: InstrumentConfig(conf, inst) for inst in supported_instruments}
    # These four groups are for the small dataset used in the upload.py test
    hsc_upload_detectors = {visit: InstrumentConfig.detector_load(conf, f"HSC-TEST-{visit}")
                            for visit in {59134, 59142, 59150, 59160}}

    # Start Prometheus endpoint
    start_http_server(8000)

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=kafka_cluster,
        group_id=group_id,
        auto_offset_reset=offset,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=sasl_username,
        sasl_plain_password=sasl_password,
    )

    gauges = {inst: Metrics(inst) for inst in supported_instruments}

    await consumer.start()

    tasks = set()

    async with httpx.AsyncClient() as client:

        try:
            # Setup kafka schema registry connection and deserialzer
            registry_api = RegistryApi(
                http_client=client, url=kafka_schema_registry_url
            )
            deserializer = Deserializer(registry=registry_api)

            while True:  # run continously
                async for msg in consumer:

                    next_visit_message_initial = await deserializer.deserialize(
                        data=msg.value
                    )

                    logging.info(f"message deserialized {next_visit_message_initial}")

                    if not next_visit_message_initial["message"]["instrument"]:
                        logging.info("Message does not have an instrument. Assuming "
                                     "it's not an observation.")
                        continue

                    # efdStamp is visit publication, in seconds since 1970-01-01 UTC
                    if next_visit_message_initial["message"]["private_efdStamp"]:
                        published = next_visit_message_initial["message"]["private_efdStamp"]
                        age = round(time.time() - published)  # Microsecond precision is distracting
                        if age > expire:
                            logging.warning("Message published on %s UTC is %s old, ignoring.",
                                            time.ctime(published),
                                            datetime.timedelta(seconds=age)
                                            )
                            continue
                    else:
                        logging.warning("Message does not have private_efdStamp, can't determine age.")

                    next_visit_message_updated = NextVisitModel(
                        salIndex=next_visit_message_initial["message"]["salIndex"],
                        scriptSalIndex=next_visit_message_initial["message"][
                            "scriptSalIndex"
                        ],
                        instrument=next_visit_message_initial["message"]["instrument"],
                        groupId=next_visit_message_initial["message"]["groupId"],
                        coordinateSystem=next_visit_message_initial["message"][
                            "coordinateSystem"
                        ],
                        position=next_visit_message_initial["message"]["position"],
                        startTime=next_visit_message_initial["message"]["startTime"],
                        rotationSystem=next_visit_message_initial["message"][
                            "rotationSystem"
                        ],
                        cameraAngle=next_visit_message_initial["message"][
                            "cameraAngle"
                        ],
                        filters=next_visit_message_initial["message"]["filters"],
                        dome=next_visit_message_initial["message"]["dome"],
                        duration=next_visit_message_initial["message"]["duration"],
                        nimages=next_visit_message_initial["message"]["nimages"],
                        survey=next_visit_message_initial["message"]["survey"],
                        totalCheckpoints=next_visit_message_initial["message"][
                            "totalCheckpoints"
                        ],
                        private_sndStamp=next_visit_message_initial["message"][
                            "private_sndStamp"
                        ],
                    )

                    match next_visit_message_updated.instrument:
                        case "LSSTComCam" | "LSSTCam" as instrument:
                            logging.info(f"Ignore {instrument} message {next_visit_message_updated}"
                                         " as the prompt service for this is not yet deployed.")
                            continue
                        case "HSC":
                            # HSC has extra active detector configurations just for the
                            # upload.py test.
                            match next_visit_message_updated.salIndex:
                                case 999:  # HSC datasets from using upload_from_repo.py
                                    gauges["HSC"].total_received.inc()
                                    send_info = fan_out(next_visit_message_updated, instruments["HSC"])
                                case visit if visit in hsc_upload_detectors:  # upload.py test datasets
                                    gauges["HSC"].total_received.inc()
                                    send_info = fan_out_hsc(next_visit_message_updated, instruments["HSC"],
                                                            hsc_upload_detectors[visit])
                                case _:
                                    raise RuntimeError("No matching case for HSC "
                                                       f"salIndex {next_visit_message_updated.salIndex}")
                        case instrument if instrument in supported_instruments:
                            gauges[instrument].total_received.inc()
                            send_info = fan_out(next_visit_message_updated, instruments[instrument])
                        case _:
                            raise RuntimeError(
                                f"no matching case for instrument {next_visit_message_updated.instrument}."
                            )

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
                                )
                            )
                            tasks.add(task)
                            task.add_done_callback(tasks.discard)

                    except ValueError as e:
                        logging.info("Error ", e)

        finally:
            await consumer.stop()


asyncio.run(main())

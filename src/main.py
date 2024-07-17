import asyncio
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
    position: typing.List[int]
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
        message: dict,
        active_detectors: list,
    ) -> list[dict[str, str]]:
        """Adds and duplicates next visit messages for fanout.

        Parameters
        ----------
        message: `str`
            The next visit message.
        active_detectors: `list`
            The active detectors for an instrument.
        Yields
        ------
        message_list : `list`
            The message list for fan out.
        """
        message_list: list[dict[str, str]] = []
        for active_detector in active_detectors:
            temp_message = message.copy()
            temp_message["detector"] = active_detector
            message_list.append(temp_message)
        return message_list


def detector_load(conf: dict, instrument: str) -> list[int]:
    """Load active instrument detectors from yaml configiration file of
    true false values for each detector.

    Parameters
    ----------
    conf : `dict`
        The instrument configuration from the yaml file.
    instrument: `str`
        The instrument to load detectors for.
    Yields
    ------
    active_detectors : `list`
        The active detectors for the instrument.
    """

    detectors = conf[instrument]["detectors"]
    active_detectors: list[int] = []
    for k, v in detectors.items():
        if v:
            active_detectors.append(k)
    return active_detectors


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
    in_process_requests_gauge.inc()

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

    in_process_requests_gauge.dec()


async def main() -> None:

    # Get environment variables
    detector_config_file = os.environ["DETECTOR_CONFIG_FILE"]
    kafka_cluster = os.environ["KAFKA_CLUSTER"]
    group_id = os.environ["CONSUMER_GROUP"]
    topic = os.environ["NEXT_VISIT_TOPIC"]
    offset = os.environ["OFFSET"]
    expire = float(os.environ["MESSAGE_EXPIRATION"])
    kafka_schema_registry_url = os.environ["KAFKA_SCHEMA_REGISTRY_URL"]
    latiss_knative_serving_url = os.environ["LATISS_KNATIVE_SERVING_URL"]
    lsstcomcam_knative_serving_url = os.environ["LSSTCOMCAM_KNATIVE_SERVING_URL"]
    lsstcomcamsim_knative_serving_url = os.environ["LSSTCOMCAMSIM_KNATIVE_SERVING_URL"]
    lsstcam_knative_serving_url = os.environ["LSSTCAM_KNATIVE_SERVING_URL"]
    hsc_knative_serving_url = os.environ["HSC_KNATIVE_SERVING_URL"]

    # kafka auth
    sasl_username = os.environ["SASL_USERNAME"]
    sasl_password = os.environ["SASL_PASSWORD"]
    sasl_mechanism = os.environ["SASL_MECHANISM"]
    security_protocol = os.environ["SECURITY_PROTOCOL"]

    # Logging config
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logging.basicConfig(stream=sys.stderr, level=logging.WARNING)

    conf = yaml.safe_load(Path(detector_config_file).read_text())

    # list based on keys in config.  Data class
    latiss_active_detectors = detector_load(conf, "LATISS")
    lsstcomcam_active_detectors = detector_load(conf, "LSSTComCam")
    lsstcam_active_detectors = detector_load(conf, "LSSTCam")
    hsc_active_detectors = detector_load(conf, "HSC")
    # These four groups are for the small dataset used in the upload.py test
    hsc_active_detectors_59134 = detector_load(conf, "HSC-TEST-59134")
    hsc_active_detectors_59142 = detector_load(conf, "HSC-TEST-59142")
    hsc_active_detectors_59150 = detector_load(conf, "HSC-TEST-59150")
    hsc_active_detectors_59160 = detector_load(conf, "HSC-TEST-59160")

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

    latiss_gauge = Gauge(
        "latiss_next_visit_messages", "next visit nessages with latiss as instrument"
    )
    lsstcam_gauge = Gauge(
        "lsstcam_next_visit_messages", "next visit nessages with lsstcam as instrument"
    )
    lsstcomcam_gauge = Gauge(
        "lsstcomcam_next_visit_messages",
        "next visit nessages with lsstcomcam as instrument",
    )
    lsstcomcamsim_gauge = Gauge(
        "lsstcomcamsim_next_visit_messages",
        "next visit nessages with lsstcomcamsim as instrument",
    )
    hsc_gauge = Gauge(
        "hsc_next_visit_messages", "next visit nessages with hsc as instrument"
    )
    hsc_in_process_requests_gauge = Gauge(
        "hsc_prompt_processing_in_process_requests",
        "hsc in process requests for next visit",
    )

    latiss_in_process_requests_gauge = Gauge(
        "latiss_prompt_processing_in_process_requests",
        "latiss in process requests for next visit",
    )

    lsstcam_in_process_requests_gauge = Gauge(
        "lsstcam_prompt_processing_in_process_requests",
        "lsstcam in process requests for next visit",
    )

    lsstcomcam_in_process_requests_gauge = Gauge(
        "lsstcomcam_prompt_processing_in_process_requests",
        "lsstcomcam in process requests for next visit",
    )

    lsstcomcamsim_in_process_requests_gauge = Gauge(
        "lsstcomcamsim_prompt_processing_in_process_requests",
        "lsstcomcamsim in process requests for next visit",
    )

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
                        case "LATISS":
                            latiss_gauge.inc()
                            fan_out_message_list = (
                                next_visit_message_updated.add_detectors(
                                    dataclasses.asdict(next_visit_message_updated),
                                    latiss_active_detectors,
                                )
                            )
                            knative_serving_url = latiss_knative_serving_url
                            in_process_requests_gauge = latiss_in_process_requests_gauge
                        case "LSSTComCamSim":
                            lsstcomcamsim_gauge.inc()
                            fan_out_message_list = (
                                next_visit_message_updated.add_detectors(
                                    dataclasses.asdict(next_visit_message_updated),
                                    # Just use ComCam active detector config.
                                    lsstcomcam_active_detectors,
                                )
                            )
                            knative_serving_url = lsstcomcamsim_knative_serving_url
                            in_process_requests_gauge = lsstcomcamsim_in_process_requests_gauge
                        case "LSSTComCam":
                            logging.info(f"Ignore LSSTComCam message {next_visit_message_updated}"
                                         " as the prompt service for this is not yet deployed.")
                            continue
                        case "LSSTCam":
                            logging.info(f"Ignore LSSTCam message {next_visit_message_updated}"
                                         " as the prompt service for this is not yet deployed.")
                            continue
                        case "HSC":
                            # HSC has extra active detector configurations just for the
                            # upload.py test.
                            match next_visit_message_updated.salIndex:
                                case 999:  # HSC datasets from using upload_from_repo.py
                                    hsc_gauge.inc()
                                    fan_out_message_list = (
                                        next_visit_message_updated.add_detectors(
                                            dataclasses.asdict(next_visit_message_updated),
                                            hsc_active_detectors,
                                        )
                                    )
                                    knative_serving_url = hsc_knative_serving_url
                                    in_process_requests_gauge = hsc_in_process_requests_gauge
                                case 59134:  # HSC upload.py test dataset
                                    hsc_gauge.inc()
                                    fan_out_message_list = (
                                        next_visit_message_updated.add_detectors(
                                            dataclasses.asdict(next_visit_message_updated),
                                            hsc_active_detectors_59134,
                                        )
                                    )
                                    knative_serving_url = hsc_knative_serving_url
                                    in_process_requests_gauge = hsc_in_process_requests_gauge
                                case 59142:  # HSC upload.py test dataset
                                    hsc_gauge.inc()
                                    fan_out_message_list = (
                                        next_visit_message_updated.add_detectors(
                                            dataclasses.asdict(next_visit_message_updated),
                                            hsc_active_detectors_59142,
                                        )
                                    )
                                    knative_serving_url = hsc_knative_serving_url
                                    in_process_requests_gauge = hsc_in_process_requests_gauge
                                case 59150:  # HSC upload.py test dataset
                                    hsc_gauge.inc()
                                    fan_out_message_list = (
                                        next_visit_message_updated.add_detectors(
                                            dataclasses.asdict(next_visit_message_updated),
                                            hsc_active_detectors_59150,
                                        )
                                    )
                                    knative_serving_url = hsc_knative_serving_url
                                    in_process_requests_gauge = hsc_in_process_requests_gauge
                                case 59160:  # HSC upload.py test dataset
                                    hsc_gauge.inc()
                                    fan_out_message_list = (
                                        next_visit_message_updated.add_detectors(
                                            dataclasses.asdict(next_visit_message_updated),
                                            hsc_active_detectors_59160,
                                        )
                                    )
                                    knative_serving_url = hsc_knative_serving_url
                                    in_process_requests_gauge = hsc_in_process_requests_gauge
                        case _:
                            raise Exception(
                                f"no matching case for instrument {next_visit_message_updated.instrument}."
                            )

                    try:
                        attributes = {
                            "type": "com.example.kafka",
                            "source": topic,
                        }

                        for fan_out_message in fan_out_message_list:
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
                                    in_process_requests_gauge,
                                    client,
                                    knative_serving_url,
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

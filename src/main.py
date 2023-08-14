import json
import logging
import os
import sys
import asyncio
import httpx
import yaml
import typing
import dataclasses
from aiokafka import AIOKafkaConsumer  # type:ignore
from cloudevents.conversion import to_structured
from cloudevents.http import CloudEvent
from dataclasses import dataclass
from pathlib import Path
from kafkit.registry.httpx import RegistryApi
from kafkit.registry import Deserializer
from prometheus_client import start_http_server, Summary  # type:ignore
from prometheus_client import Gauge

REQUEST_TIME = Summary("request_processing_seconds", "Time spent processing request")


@dataclass
class NextVisitModel:
    "Next Visit Message"
    salIndex: int
    scriptSalIndex: int
    groupId: str
    coordinateSystem: int
    position: typing.List[int]
    rotationSystem: int
    cameraAngle: float
    filters: str
    dome: int
    duration: float
    nimages: int
    survey: str
    totalCheckpoints: int

    def add_detectors(
        self,
        instrument: str,
        message: dict,
        active_detectors: list,
    ) -> list[dict[str, str]]:
        """Adds and duplicates next visit messages for fanout.

        Parameters
        ----------
        instrument: `str`
            The instrument to load detectors for.
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
            temp_message["instrument"] = instrument
            temp_message["detector"] = active_detector
            # temporary change to modify short filter names to format expected by butler
            if temp_message["filters"] != "" and len(temp_message["filters"]) == 1:
                temp_message["filters"] = (
                    "SDSS" + temp_message["filters"] + "_65mm~empty"
                )
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
    next_visit_group_id,
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
    """
    in_process_requests_gauge.inc()

    result = await client.post(
        knative_serving_url,
        headers=headers,
        data=body,  # type:ignore
        timeout=None,
    )

    logging.info(
        f"group id {next_visit_group_id} status code {result.status_code} for initial request {result.content}"
    )

    if result.status_code == 502 or result.status_code == 503:
        logging.info(
            f"retry after status code {result.status_code} for group id {next_visit_group_id}"
        )
        retry_result = await client.post(
            knative_serving_url,
            headers=headers,
            data=body,  # type:ignore
            timeout=None,
        )
        logging.info(
            f"group id {next_visit_group_id} retried request {retry_result.content}"
        )

    in_process_requests_gauge.dec()


async def main() -> None:

    # Get environment variables
    detector_config_file = os.environ["DETECTOR_CONFIG_FILE"]
    kafka_cluster = os.environ["KAFKA_CLUSTER"]
    group_id = os.environ["CONSUMER_GROUP"]
    topic = os.environ["NEXT_VISIT_TOPIC"]
    offset = os.environ["OFFSET"]
    kafka_schema_registry_url = os.environ["KAFKA_SCHEMA_REGISTRY_URL"]
    latiss_knative_serving_url = os.environ["LATISS_KNATIVE_SERVING_URL"]
    lsst_cam_knative_serving_url = os.environ["LSST_CAM_KNATIVE_SERVING_URL"]
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
    lsst_com_cam_active_detectors = detector_load(conf, "LSSTComCam")
    lsst_cam_active_detectors = detector_load(conf, "LSSTCam")
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

                    next_visit_message_updated = NextVisitModel(
                        salIndex=next_visit_message_initial["message"]["salIndex"],
                        scriptSalIndex=next_visit_message_initial["message"][
                            "scriptSalIndex"
                        ],
                        groupId=next_visit_message_initial["message"]["groupId"],
                        coordinateSystem=next_visit_message_initial["message"][
                            "coordinateSystem"
                        ],
                        position=next_visit_message_initial["message"]["position"],
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
                    )

                    match next_visit_message_updated.salIndex:
                        case 2:  # LATISS
                            latiss_gauge.inc()
                            fan_out_message_list = (
                                next_visit_message_updated.add_detectors(
                                    "LATISS",
                                    dataclasses.asdict(next_visit_message_updated),
                                    latiss_active_detectors,
                                )
                            )
                            knative_serving_url = latiss_knative_serving_url
                            in_process_requests_gauge = latiss_in_process_requests_gauge
                        # case "LSSTComCam":
                        #    fan_out_message_list = next_visit_message.add_detectors(
                        #        "LSSTComCam", next_visit_message, lsst_com_cam_active_detectors
                        #    )
                        case 1:  # LSSTCam
                            lsstcam_gauge.inc()
                            fan_out_message_list = (
                                next_visit_message_updated.add_detectors(
                                    "LSSTCam",
                                    dataclasses.asdict(next_visit_message_updated),
                                    lsst_cam_active_detectors,
                                )
                            )
                            knative_serving_url = lsst_cam_knative_serving_url
                            in_process_requests_gauge = (
                                lsstcam_in_process_requests_gauge
                            )
                        case 999:  # HSC
                            hsc_gauge.inc()
                            fan_out_message_list = (
                                next_visit_message_updated.add_detectors(
                                    "HSC",
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
                                    "HSC",
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
                                    "HSC",
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
                                    "HSC",
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
                                    "HSC",
                                    dataclasses.asdict(next_visit_message_updated),
                                    hsc_active_detectors_59160,
                                )
                            )
                            knative_serving_url = hsc_knative_serving_url
                            in_process_requests_gauge = hsc_in_process_requests_gauge
                        case _:
                            raise Exception(
                                f"no matching case for salIndex {next_visit_message_updated.salIndex} to add instrument value"
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

                            task = asyncio.create_task(
                                knative_request(
                                    in_process_requests_gauge,
                                    client,
                                    knative_serving_url,
                                    headers,
                                    body,
                                    next_visit_message_updated.groupId,
                                )
                            )
                            tasks.add(task)
                            task.add_done_callback(tasks.discard)

                    except ValueError as e:
                        logging.info("Error ", e)

        finally:
            await consumer.stop()


asyncio.run(main())

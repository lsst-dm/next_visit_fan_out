#!/bin/sh

curl --request POST \
  --url https://usdf-rsp-dev.slac.stanford.edu/sasquatch-rest-proxy/topics/test.next-visit \
  --header 'Content-Type: application/vnd.kafka.avro.v2+json' \
  --data '{"value_schema": "{\"type\":\"record\",\"name\":\"logevent_nextVisit\",\"namespace\":\"lsst.sal.ScriptQueue\",\"fields\":[{\"name\":\"private_efdStamp\",\"type\":\"double\",\"default\":0.0,\"description\":\"UTC time for EFD timestamp. An integer (the number of leap seconds) different from private_sndStamp.\",\"units\":\"second\"},{\"name\":\"private_kafkaStamp\",\"type\":\"double\",\"default\":0.0,\"description\":\"TAI time at which the Kafka message was created.\",\"units\":\"second\"},{\"name\":\"salIndex\",\"type\":\"long\",\"default\":0,\"description\":\"Index number for CSC with multiple instances\",\"units\":\"unitless\"},{\"name\":\"private_revCode\",\"type\":\"string\",\"default\":\"\",\"description\":\"Revision hashcode\",\"units\":\"unitless\"},{\"name\":\"private_sndStamp\",\"type\":\"double\",\"default\":0.0,\"description\":\"Time of instance publication\",\"units\":\"second\"},{\"name\":\"private_rcvStamp\",\"type\":\"double\",\"default\":0.0,\"description\":\"Time of instance reception\",\"units\":\"second\"},{\"name\":\"private_seqNum\",\"type\":\"long\",\"default\":0,\"description\":\"Sequence number\",\"units\":\"unitless\"},{\"name\":\"private_identity\",\"type\":\"string\",\"default\":\"\",\"description\":\"Identity of publisher\",\"units\":\"unitless\"},{\"name\":\"private_origin\",\"type\":\"long\",\"default\":0,\"description\":\"PID of publisher\",\"units\":\"unitless\"},{\"name\":\"scriptSalIndex\",\"type\":\"long\",\"default\":0,\"description\":\"Index of Script SAL component.\",\"units\":\"unitless\"},{\"name\":\"groupId\",\"type\":\"string\",\"default\":\"\",\"description\":\"Group ID for images taken by the script.\",\"units\":\"unitless\"},{\"name\":\"coordinateSystem\",\"type\":\"long\",\"default\":0,\"description\":\"Coordinate system for position. A Script MetadataCoordSys enum. Observed is refracted apparent topocentric, e.g. similar to Mount but the telescope model is applied and the azimuth is wrapped. If None then position should be ignored.\",\"units\":\"unitless\"},{\"name\":\"position\",\"type\":{\"type\":\"array\",\"items\":\"double\"},\"default\":[0.0,0.0],\"description\":\"Longitude, latitude axes of position in coordinateSystem\",\"units\":\"degree\"},{\"name\":\"startTime\",\"type\":\"double\",\"default\":0.0,\"description\":\"The expected start time for this visit (TAI).\",\"units\":\"degree\"},{\"name\":\"rotationSystem\",\"type\":\"long\",\"default\":0,\"description\":\"<A HREF=https://ts-xml.lsst.io/python/lsst/ts/xml/data/sal_interfaces/ScriptQueue.html#nextvisit>ScriptQueue_ScriptQueue_logevent_nextVisit</A>\",\"units\":\"unitless\"},{\"name\":\"cameraAngle\",\"type\":\"double\",\"default\":0.0,\"description\":\"Camera angle in rotationSystem\",\"units\":\"degree\"},{\"name\":\"filters\",\"type\":\"string\",\"default\":\"\",\"description\":\"Comma-separated names of acceptable filters, or blank for any filter.\",\"units\":\"unitless\"},{\"name\":\"dome\",\"type\":\"long\",\"default\":0,\"description\":\"Desired dome state; a Script MetadataDome enum.\",\"units\":\"unitless\"},{\"name\":\"duration\",\"type\":\"double\",\"default\":0.0,\"description\":\"Estimated duration of the script, excluding slewing to the initial position required by the script.\",\"units\":\"second\"},{\"name\":\"nimages\",\"type\":\"long\",\"default\":0,\"description\":\"Predicted number of images to take; 0 if unknown.\",\"units\":\"unitless\"},{\"name\":\"instrument\",\"type\":\"string\",\"default\":\"\",\"description\":\"Instrument name (the short name used by middleware) used to take the data; blank if unknown or not relevant.\",\"units\":\"unitless\"},{\"name\":\"survey\",\"type\":\"string\",\"default\":\"\",\"description\":\"Survey name.\",\"units\":\"unitless\"},{\"name\":\"totalCheckpoints\",\"type\":\"long\",\"default\":0,\"description\":\"Predicted total number of checkpoints that will be seen (counting all repetitions of a repeated checkpoint); 0 if unknown.\",\"units\":\"unitless\"}],\"description\":\"Group ID and other information about the next script to be run.\",\"sal_version\":\"7.4.0\",\"xml_version\":\"19.0.0\"}",
"records": [
	{
		"value": {
			"private_efdStamp": 1674516757.7661333,
			"private_kafkaStamp": 1674516794.7740011,
			"salIndex": 999,
			"private_revCode": "c9aab3df",
			"private_sndStamp": 1674516794.7661333,
			"private_rcvStamp": 1674516794.7662647,
			"private_seqNum": 22,
			"private_identity": "ScriptQueue:2",
			"private_origin": 2786567,
			"scriptSalIndex": 200023,
			"groupId": "2023-01-23T23:33:14.762",
			"coordinateSystem": 1,
			"position": [
				0,
				0
			],
			"startTime": 1674516800.0,
			"rotationSystem": 1,
			"cameraAngle": 0,
			"filters": "",
			"dome": 3,
			"duration": 2,
			"nimages": 0,
			"survey": "",
			"totalCheckpoints": 0,
			"instrument": "test"
		}
	}
]
}'

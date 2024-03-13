import asyncio
import collections
import os
import json

import boto3
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.batch import AsyncBatchProcessor, EventType, async_process_partial_response
from aws_lambda_powertools.utilities.batch.types import PartialItemFailureResponse
from aws_lambda_powertools.utilities.data_classes.dynamo_db_stream_event import DynamoDBStreamEvent, DynamoDBRecord
from aws_lambda_powertools.utilities.typing import LambdaContext

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
AWS_REGION = os.environ['AWS_REGION']
TOPIC_ARN = os.environ['PUBLISH_TOPIC_ARN']

logger = Logger(service="stream-reader", level=LOG_LEVEL, log_uncaught_exceptions=True)
processor = AsyncBatchProcessor(event_type=EventType.DynamoDBStreams)
sns = boto3.client("sns", region_name=AWS_REGION)

Message = collections.namedtuple(
    typename="Message",
    field_names=[
        "subject",
        "message",
        "attributes"
    ]
)


async def publish_message(message: Message) -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda: sns.publish(
        TopicArn=TOPIC_ARN,
        Message=message.message,
        Subject=message.subject,
        MessageAttributes=message.attributes,
        MessageStructure="string"
    ))


def prepare_message(record: DynamoDBRecord) -> Message:
    return Message(
        subject=f"{record.event_name}-{record.event_id}",
        message=json.dumps(record.raw_event),
        attributes={
            "Operation": {
                "DataType": "String",
                "StringValue": str(record.event_name or "UNKNOWN")
            }
        }
    )


async def record_handler(record: DynamoDBRecord) -> None:
    message = prepare_message(record)
    await publish_message(message)


@logger.inject_lambda_context
def lambda_handler(event: DynamoDBStreamEvent, context: LambdaContext) -> PartialItemFailureResponse:
    response = async_process_partial_response(
        event=event,  # type: ignore
        record_handler=record_handler,
        processor=processor,
        context=context
    )
    logger.debug(response)
    return response

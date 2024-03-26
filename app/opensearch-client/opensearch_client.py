import json
import os
from typing import Tuple

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.batch import AsyncBatchProcessor, EventType, async_process_partial_response
from aws_lambda_powertools.utilities.batch.types import PartialItemFailureResponse
from aws_lambda_powertools.utilities.data_classes.dynamo_db_stream_event import DynamoDBRecord, DynamoDBRecordEventName
from aws_lambda_powertools.utilities.data_classes.sns_event import SNSMessage
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord, SQSEvent
from aws_lambda_powertools.utilities.typing import LambdaContext
from pydantic import BaseModel, Field, ConfigDict

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
AWS_REGION = os.environ['AWS_REGION']

logger = Logger(service="opensearch-client", level=LOG_LEVEL, log_uncaught_exceptions=True)
processor = AsyncBatchProcessor(event_type=EventType.SQS)


class Image(BaseModel):
    model_config = ConfigDict(extra='allow')

    PK: str = Field(strict=True)


def unwrap_dynamodb_record(record: SQSRecord) -> Tuple[DynamoDBRecordEventName, Image]:
    """
    Unwraps the original DynamoDB Stream record and parses it as Image object.

    Assumes that after DynamoDB CDC event is published, it goes through SNS, then into SQS
    and finally ends up in this lambda function.

    :param record: a single message from SQS
    :return: a tuple of DynamoDB image and operation
    """

    sns_message: SNSMessage = json.loads(record.json_body, object_hook=SNSMessage)
    dynamodb_record: DynamoDBRecord = json.loads(sns_message.message, object_hook=DynamoDBRecord)

    event_name: DynamoDBRecordEventName = dynamodb_record.event_name
    image: Image = Image(**dynamodb_record.dynamodb.new_image)

    return event_name, image


async def record_handler(record: SQSRecord) -> None:
    operation, image = unwrap_dynamodb_record(record)

    logger.debug(operation)
    logger.debug(image)


@logger.inject_lambda_context
def lambda_handler(event: SQSEvent, context: LambdaContext) -> PartialItemFailureResponse:
    logger.debug(event)
    response = async_process_partial_response(
        event=event,  # type: ignore
        record_handler=record_handler,
        processor=processor,
        context=context
    )
    logger.debug(response)
    return response

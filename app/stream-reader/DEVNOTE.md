## Development notes:
### 2024-03-01
Right now we're using batch processor, which handles each DynamoDB record individually:
```python
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
```
Possible improvements:
- the better approach would be aggregating those events and publishing an entire batch to SNS
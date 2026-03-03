import logging
import os
import boto3
from config import config

ACCESS_KEY_ID = config.aws.access_key_id
SECRET_ACCESS_KEY = config.aws.secret_access_key
REGION_NAME = config.aws.region
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", None)

logger = logging.getLogger(__name__)


class SQSClient:
    def __init__(self):
        if not ACCESS_KEY_ID:
            raise ValueError(
                "AWS_ACCESS_KEY_ID environment variable is not set.")
        if not SECRET_ACCESS_KEY:
            raise ValueError(
                "AWS_SECRET_ACCESS_KEY environment variable is not set.")
        if not SQS_QUEUE_URL:
            raise ValueError("SQS_QUEUE_URL environment variable is not set.")

        self._client = boto3.client(
            "sqs",
            region_name=REGION_NAME
        )
        self.queue_url = SQS_QUEUE_URL

    def poll_message(self, wait_time: int = 10, max_messages: int = 1):
        """
        Poll messages from the SQS queue.

        Args:
            wait_time: Long polling wait time in seconds
            max_messages: Maximum number of messages to retrieve
        Returns:
            List of messages retrieved from the queue
        """
        logger.info(f"Polling messages from SQS queue: {self.queue_url}")
        response = self._client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time
        )
        messages = response.get("Messages", [])
        logger.info(f"Received {len(messages)} messages from SQS queue.")
        return messages

    def delete_message(self, receipt_handle: str) -> None:
        """
        Delete a message from the SQS queue.

        Args:
            receipt_handle: The receipt handle of the message to delete
        """
        logger.info(f"Deleting message from SQS queue: {self.queue_url}")
        self._client.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle
        )
        logger.info("Message deleted successfully.")

    def send_message(self, message_body: str, delay_seconds: int = 0) -> None:
        """
        Send a message to the SQS queue.

        Args:
            message_body: The body of the message to send
            delay_seconds: Delay in seconds before the message becomes visible
        """
        logger.info(f"Sending message to SQS queue: {self.queue_url}")
        self._client.send_message(
            QueueUrl=self.queue_url,
            MessageBody=message_body,
            DelaySeconds=delay_seconds
        )
        logger.info("Message sent successfully.")

    def extend_message_visibility(self, receipt_handle: str, visibility_timeout: int) -> None:
        """
        Extend the visibility timeout of a message in the SQS queue. This prevents other consumers from processing the message for a longer period.

        Args:
            receipt_handle: The receipt handle of the message
            visibility_timeout: New visibility timeout in seconds
        """
        self._client.change_message_visibility(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=visibility_timeout
        )
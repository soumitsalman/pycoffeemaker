import mimetypes
import os
from pathlib import Path
import asyncio
import boto3
import aioboto3
from botocore.client import Config

_CONFIG = Config(s3={'addressing_style': 'virtual'})
_MAX_CONCURRENCY = 100

def _guess_type(file_path: str) -> str:
    content_type, _ = mimetypes.guess_type(file_path)
    return content_type or 'application/octet-stream'

class CDNStore:
    def __init__(self, bucket: str, public_access_url: str = None):
        self.bucket = bucket.removeprefix("s3://").removesuffix("/")
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("S3_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY"),
            region_name=os.getenv("S3_REGION"),
            endpoint_url=os.getenv("S3_ENDPOINT")
        )        
        # self.endpoint_url = os.getenv("S3_ENDPOINT")
        self.public_url = public_access_url.rstrip("/") if public_access_url else None

    def upload_text(self, path: str, content: str) -> str:
        """Uploads a single text file.

        Parameters:
            path: should be in the format 'folder/file_name.ext'.
            content: is the text content to be uploaded.
        """
        self.s3_client.put_object(
            Bucket=self.bucket, 
            Key=path, 
            Body=content.encode('utf-8'), 
            ContentType=_guess_type(path)
        )
        return _public_url(self.public_url, path)

    def upload_binary(self, path: str, data: bytes) -> str:
        """Uploads a single binary file.

        Parameters:
            path: should be in the format 'folder/file_name.ext'.
            data: is the binary content to be uploaded.
        """
        self.s3_client.put_object(
            Bucket=self.bucket, 
            Key=path, 
            Body=data, 
            ContentType=_guess_type(path)
        )
        return _public_url(self.public_url, path)

    # async def batch_upload_texts(self, data: list[dict]) -> dict[str, str]:
    #     """Uploads multiple text items concurrently. 
    #     Parameters:
    #         data: A list of dictionaries, each containing 'path' and 'content' keys.
    #             'path' should be in the format 'folder/file_name.ext'.
    #             'content' is the text content to be uploaded.
    #     """
    #     async with self.session.client('s3', endpoint_url=self.endpoint_url, config=_CONFIG) as s3:
    #         return await asyncio.gather(*(self._upload(s3, item['path'], item['content']) for item in data))


class AsyncCDNStore:
    def __init__(self, bucket: str, public_access_url: str = None, max_concurrency: int = _MAX_CONCURRENCY):
        self.bucket = bucket.removeprefix("s3://").removesuffix("/")
        self.session = aioboto3.Session(
            aws_access_key_id=os.getenv("S3_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY"),
            region_name=os.getenv("S3_REGION")
        )
        self.endpoint_url = os.getenv("S3_ENDPOINT")
        self.public_url = public_access_url.rstrip("/") if public_access_url else None
        self.throttle = asyncio.Semaphore(max_concurrency) 

    async def _upload(self, s3_client, key: str, content: bytes) -> str:
        async with self.throttle:
            await s3_client.put_object(
                Bucket=self.bucket, 
                Key=key, 
                Body=content.encode('utf-8'), 
                ContentType="text/plain; charset=utf-8"
            )
        return _public_url(self.public_url, key)

    async def upload_text(self, path: str, content: str) -> str:
        """Uploads a single text file.

        Parameters:
            path: should be in the format 'folder/file_name.ext'.
            content: is the text content to be uploaded.
        """
        async with self.session.client('s3', endpoint_url=self.endpoint_url, config=_CONFIG) as s3_client:
            return await self._upload(s3_client, path, content)

    async def upload_binary(self, path: str, data: bytes) -> str:
        """Uploads a single binary file.

        Parameters:
            path: should be in the format 'folder/file_name.ext'.
            data: is the binary content to be uploaded.
        """
        async with self.session.client('s3', endpoint_url=self.endpoint_url, config=_CONFIG) as s3_client:
            async with self.throttle:
                await s3_client.put_object(
                    Bucket=self.bucket, 
                    Key=path, 
                    Body=data, 
                    ContentType=_guess_type(path)
                )
        return _public_url(self.public_url, path)

    async def batch_upload_texts(self, data: list[dict]) -> dict[str, str]:
        """Uploads multiple text items concurrently. 
        Parameters:
            data: A list of dictionaries, each containing 'path' and 'content' keys.
                'path' should be in the format 'folder/file_name.ext'.
                'content' is the text content to be uploaded.
        """
        async with self.session.client('s3', endpoint_url=self.endpoint_url, config=_CONFIG) as s3:
            return await asyncio.gather(*(self._upload(s3, item['path'], item['content']) for item in data))

def _public_url(public_url: str, key: str) -> str:
    """Creates a public access URL based on template. Ex: https://{bucket}.t3.tigrisfiles.io/{key}"""
    return f"{public_url}/{key}" if public_url else key
        


from contextlib import asynccontextmanager
from aiobotocore.session import get_session
from fastapi import UploadFile
from src.config import settings
from io import BytesIO
from PIL import Image
import mozjpeg_lossless_optimization
import uuid


class ImageOptimizator:

    @staticmethod
    async def convert_to_optimized_jpeg(file_bytes: bytes) -> bytes:
        jpeg_io = BytesIO()

        image = Image.open(BytesIO(file_bytes))

        image.convert('RGB').save(jpeg_io, format='JPEG', quality=25, optimize=True)

        jpeg_io.seek(0)
        jpeg_bytes = jpeg_io.read()

        optimized_jpeg_bytes = mozjpeg_lossless_optimization.optimize(jpeg_bytes)

        return optimized_jpeg_bytes

    @staticmethod
    async def convert_to_webp(file_bytes: bytes, optimize: bool = False) -> bytes:
        webp_io = BytesIO()

        image = Image.open(BytesIO(file_bytes))

        if image.mode in ('RGBA', 'LA') or (image.mode == 'P' and 'transparency' in image.info):
            image = image.convert('RGBA')

        if optimize:
            image.save(webp_io, format='WEBP', quality=50, method=6, optimize=True)
        else:
            image.save(webp_io, format='WEBP')

        webp_io.seek(0)
        return webp_io.read()

class S3Client:
    def __init__(
            self,
            access_key: str = settings.ACCESS_KEY,
            secret_key: str = settings.SECRET_KEY,
            endpoint_url: str = settings.ENDPOINT_URL,
            bucket_name: str = settings.BUCKET_NAME,
            region: str = settings.REGION
    ):
        self.config = {
            'aws_access_key_id': access_key,
            'aws_secret_access_key': secret_key,
            'endpoint_url': endpoint_url,
            'region_name': region
        }
        self.bucket_name = bucket_name
        self.session = get_session()

    @asynccontextmanager
    async def get_client(self):
        async with self.session.create_client('s3', **self.config) as client:
            yield client

    async def upload_file(self, file: bytes) -> str:
        async with self.get_client() as client:
            
            unique_name = f'{uuid.uuid4()}.webp'
            file = await ImageOptimizator.convert_to_webp(file_bytes=file, optimize=True)
            
            # unique_name = f'{uuid.uuid4()}.jpg'
            # file = await ImageOptimizator.convert_to_optimized_jpeg(file_bytes=file)
            
            await client.put_object(Bucket=self.bucket_name, Key=unique_name, Body=file)

        return self.get_file_link(unique_name)

    async def download_file(self, object_name: str) -> bytes:
        async with self.get_client() as client:
            response = await client.get_object(Bucket=self.bucket_name, Key=object_name)
            return await response['Body'].read()

    async def delete_file(self, object_name: str) -> None:
        async with self.get_client() as client:
            await client.delete_object(Bucket=self.bucket_name, Key=object_name)

    async def update_file(self, old_link: str, new_file: UploadFile) -> str:
        object_name = old_link.split('/')[-1]
        await self.delete_file(object_name)
        new_file_content = await new_file.read()
        return await self.upload_file(new_file_content)

    def get_file_link(self, object_name: str) -> str:
        return f'{self.config['endpoint_url']}{self.bucket_name}/{object_name}'


DEFAULT_S3_CLIENT = S3Client()

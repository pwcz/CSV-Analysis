from fastapi import APIRouter, UploadFile, File
import aiofiles

from src.utils.statement import process_statement

router = APIRouter()


@router.post("/upload_file")
async def post_upload_file(in_file: UploadFile = File()):
    async with aiofiles.tempfile.NamedTemporaryFile('wb', delete=False) as out_file:
        content = await in_file.read()
        await out_file.write(content)

    return process_statement(out_file.name)

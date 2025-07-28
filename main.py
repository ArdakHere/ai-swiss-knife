from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import FileResponse
from services.core import make_request_normalize_excel_driver
import os 

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Welcome NCT In-house AI backend platform"}


@app.post("/openai_api/normalize_excel")
async def normalize_excel(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    column_name: str = Form(...),
    prompt_str: str = Form(...)
):
    temp_filename = f"temp_{file.filename}"

    with open(temp_filename, "wb") as f:
        f.write(await file.read())

    # ✅ Get path to processed file
    output_file = await make_request_normalize_excel_driver(temp_filename, column_name, prompt_str)

    # Optional cleanup (delete temp and output file after sending)
    background_tasks.add_task(os.remove, temp_filename)
    background_tasks.add_task(os.remove, output_file)

    # ✅ Return file to user
    return FileResponse(
        output_file,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=os.path.basename(output_file)
    )
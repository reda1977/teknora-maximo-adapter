"""
أداة نقل بيانات مستقلة من Maximo لتكنورا (خارج نطاق البورتال تمامًا) -
ويزرد: اتصال بـ Maximo -> اتصال بتكنورا -> معاينة الأعداد -> نقل مع
تقدم حي -> تقرير نهائي. الأداة نفسها من غير أي تسجيل دخول (بتتشغل محليًا
بواسطة الأدمن)، لكنها بتستخدم بيانات دخول حقيقية للاتصال بالنظامين.
"""
import asyncio
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from maximo_client import MaximoClient, MaximoAuthError
from teknora_client import TeknoraClient, TeknoraAuthError
from migration import MigrationRun, MIGRATION_ORDER

app = FastAPI(title="Maximo -> Teknora Migrator")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# حالة عامة بسيطة - الأداة مصممة لمستخدم واحد (الأدمن) في كل مرة، مش
# نظام متعدد المستخدمين، فمفيش داعي لتعقيد إدارة الجلسات
STATE = {
    "maximo": None,   # MaximoClient
    "teknora": None,  # TeknoraClient
    "run": None,      # MigrationRun
}


class MaximoConnectRequest(BaseModel):
    base_url: str
    username: str
    password: str


class TeknoraConnectRequest(BaseModel):
    base_url: str
    username: str
    password: str


class StartMigrationRequest(BaseModel):
    types: list[str]


def _describe_exc(e: Exception) -> str:
    """بعض الاستثناءات (زي انتهاء مهلة الاتصال) مالهاش نص وصفي في str(e) -
    فبنرجع اسم نوع الخطأ نفسه بدل رسالة فاضية غير مفيدة."""
    return str(e) or type(e).__name__


@app.post("/api/connect/maximo")
async def connect_maximo(req: MaximoConnectRequest):
    client = MaximoClient(req.base_url, req.username, req.password)
    try:
        await client.test_connection()
    except MaximoAuthError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"تعذر الاتصال بـ Maximo: {_describe_exc(e)}")
    STATE["maximo"] = client
    return {"message": "تم الاتصال بـ Maximo بنجاح"}


@app.post("/api/connect/teknora")
async def connect_teknora(req: TeknoraConnectRequest):
    client = TeknoraClient(req.base_url, req.username, req.password)
    try:
        await client.login()
    except TeknoraAuthError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"تعذر الاتصال بتكنورا: {_describe_exc(e)}")
    STATE["teknora"] = client
    return {"message": "تم الاتصال بتكنورا بنجاح"}


@app.get("/api/maximo/summary")
async def maximo_summary():
    client: MaximoClient = STATE["maximo"]
    if not client:
        raise HTTPException(status_code=400, detail="لسه متصلتش بـ Maximo")

    counts = {}
    errors = {}
    checks = {
        "organizations": "mxorganization",
        "locations": "mxoperloc",
        "assets": "mxasset",
        "jobplans": "mxjobplan",
        "workorders": "mxwo",
    }
    for type_key, os_name in checks.items():
        try:
            counts[type_key] = await client.count_collection(os_name)
        except Exception as e:
            counts[type_key] = None
            errors[type_key] = _describe_exc(e)[:300]

    return {
        "counts": counts,
        "errors": errors,
        "order": MIGRATION_ORDER,
    }


@app.post("/api/migrate/start")
async def start_migration(req: StartMigrationRequest):
    maximo: MaximoClient = STATE["maximo"]
    teknora: TeknoraClient = STATE["teknora"]
    if not maximo:
        raise HTTPException(status_code=400, detail="لسه متصلتش بـ Maximo")
    if not teknora:
        raise HTTPException(status_code=400, detail="لسه متصلتش بتكنورا")

    existing_run: MigrationRun = STATE["run"]
    if existing_run and existing_run.state["status"] == "running":
        raise HTTPException(status_code=409, detail="فيه عملية نقل شغالة بالفعل")

    run = MigrationRun(maximo, teknora, req.types)
    STATE["run"] = run
    asyncio.create_task(run.run())
    return {"message": "بدأت عملية النقل"}


@app.get("/api/migrate/progress")
async def migrate_progress():
    run: MigrationRun = STATE["run"]
    if not run:
        raise HTTPException(status_code=404, detail="مفيش عملية نقل بدأت لسه")
    return run.state


# تقديم واجهة الويزرد الثابتة (ملف HTML واحد، من غير أي خطوة build) -
# بمسار مطلق مبني على مكان الملف نفسه، عشان يشتغل مهما كان الـ working
# directory اللي اتشغل منه uvicorn
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"
app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")


@app.get("/")
async def index():
    return FileResponse(str(FRONTEND_DIR / "index.html"))

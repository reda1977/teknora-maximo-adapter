"""
أداة نقل بيانات مستقلة من Maximo لتكنورا (خارج نطاق البورتال تمامًا) -
ويزرد: اتصال بـ Maximo -> اتصال بتكنورا -> معاينة الأعداد -> نقل مع
تقدم حي -> تقرير نهائي. الأداة نفسها من غير أي تسجيل دخول (بتتشغل محليًا
بواسطة الأدمن)، لكنها بتستخدم بيانات دخول حقيقية للاتصال بالنظامين.
"""
import asyncio
import re
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from maximo_client import MaximoClient, MaximoAuthError
from teknora_client import TeknoraClient, TeknoraAuthError
from migration import (DEFAULT_BATCH_SIZE, MIGRATION_ORDER, TYPE_SPECS, MigrationRun,
                       checkpoint_key, failed_entry, load_checkpoints, save_checkpoint, save_failed)

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
    batch_size: int = DEFAULT_BATCH_SIZE


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
    for type_key, spec in TYPE_SPECS.items():
        os_name = spec.get("count_os", spec["os"])
        try:
            counts[type_key] = await client.count_collection(os_name)
        except Exception as e:
            counts[type_key] = None
            errors[type_key] = _describe_exc(e)[:300]

    all_cp = load_checkpoints()
    batched = {}
    for type_key, spec in TYPE_SPECS.items():
        if not spec.get("batch_key"):
            continue
        cp_key = checkpoint_key(client.base_url, type_key)
        fe = failed_entry(cp_key)
        batched[type_key] = {
            "batch_size": DEFAULT_BATCH_SIZE,
            "checkpoint": all_cp.get(cp_key),
            "pending_retry": len(fe["items"]),
            "legacy_pending": not fe.get("legacy_done") and fe.get("legacy_until") is not None,
        }

    return {
        "counts": counts,
        "errors": errors,
        "order": MIGRATION_ORDER,
        "batched": batched,
    }


@app.get("/api/maximo/sample/{object_structure}")
async def maximo_sample(object_structure: str, n: int = 3, where: str = None, select: str = None):
    """أول كام سجل من أي Object Structure زي ما ماكسيمو بيرجعهم بالظبط
    (بعد شيل بادئة spi: من المستوى الأول بس) - عشان نشوف الشكل الحقيقي
    للبيانات قبل ما نكتب الماپنج، بدل ما نفترضه ونكتشف الغلط بعد نقل كامل."""
    client: MaximoClient = STATE["maximo"]
    if not client:
        raise HTTPException(status_code=400, detail="لسه متصلتش بـ Maximo")
    if not re.fullmatch(r"[A-Za-z0-9_]+", object_structure):
        raise HTTPException(status_code=400, detail="اسم Object Structure غير صالح")
    try:
        # where اختياري (زي spi:wonum="WO-123") عشان نشوف سجل بعينه معروف إن
        # عليه البيانات الفرعية اللي بندوّر عليها، بدل أول سجلات عشوائية
        # select اختياري عشان نجرّب صيغة oslc.select (بما فيها القوايم
        # المتداخلة زي spi:woactivity{...}) على سيرفر ماكسيمو الحقيقي قبل ما
        # النقل نفسه يعتمد عليها
        return await client.fetch_first_page(object_structure, where=where, select=select,
                                             page_size=max(1, min(n, 20)))
    except Exception as e:
        raise HTTPException(status_code=502, detail=_describe_exc(e))


@app.delete("/api/checkpoints/{type_key}")
async def reset_checkpoint(type_key: str):
    client: MaximoClient = STATE["maximo"]
    if not client:
        raise HTTPException(status_code=400, detail="لسه متصلتش بـ Maximo")
    run: MigrationRun = STATE["run"]
    if run and run.state["status"] == "running":
        raise HTTPException(status_code=409, detail="مينفعش تصفّر نقطة الاستكمال والنقل شغال")
    cp_key = checkpoint_key(client.base_url, type_key)
    save_checkpoint(cp_key, None)
    # قايمة الفاشل بتتصفّر كمان - من غيرها "إعادة الفاشل" كانت هتبعت أرقام
    # من التشغيلة القديمة وتدوّر على أوامر COMP في مداها القديم
    save_failed(cp_key, {"items": {}, "legacy_until": None, "legacy_done": True})
    return {"message": "اتصفّرت نقطة الاستكمال وقايمة الفاشل - الدفعة الجاية هتبدأ من الأول"}


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

    if req.batch_size < 1:
        raise HTTPException(status_code=400, detail="حجم الدفعة لازم يكون أكبر من صفر")
    run = MigrationRun(maximo, teknora, req.types, batch_size=req.batch_size)
    STATE["run"] = run
    asyncio.create_task(run.run())
    return {"message": "بدأت عملية النقل"}


@app.post("/api/migrate/retry/{type_key}")
async def retry_failed(type_key: str):
    maximo: MaximoClient = STATE["maximo"]
    teknora: TeknoraClient = STATE["teknora"]
    if not maximo or not teknora:
        raise HTTPException(status_code=400, detail="لازم تتصل بـ Maximo وتكنورا الأول")
    if not TYPE_SPECS.get(type_key, {}).get("batch_key"):
        raise HTTPException(status_code=400, detail="إعادة الفاشل متاحة للأنواع المقسمة على دفعات بس")
    existing_run: MigrationRun = STATE["run"]
    if existing_run and existing_run.state["status"] == "running":
        raise HTTPException(status_code=409, detail="فيه عملية نقل شغالة بالفعل")

    run = MigrationRun(maximo, teknora, [type_key], mode="retry")
    STATE["run"] = run
    asyncio.create_task(run.run())
    return {"message": "بدأت إعادة الفاشل"}


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

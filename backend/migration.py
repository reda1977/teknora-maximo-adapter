"""
منطق النقل نفسه: ترتيب الأنواع (المنظمات/المواقع الأول عشان باقي الأنواع
بترجع ليهم بمفتاح خارجي)، تحويل حقول Maximo لحقول تكنورا، وتتبع تقدم كل
نوع (نجح/فشل/الإجمالي) عشان الويزرد يعرض بروجريس بار حي ويطلع تقرير
نهائي مفصّل.

ترتيب الهجرة إجباري بسبب الروابط بين الجداول في تكنورا:
Organizations+Sites -> Persons -> Crafts -> Labor -> Locations -> Assets ->
Meters -> Work Orders -> Meter Readings -> Job Plans -> PM

ملحوظة: أوامر الشغل بتتنقل قبل خطط العمل (بناءً على ترتيب مطلوب)، فمبنبعتش
ربط jpnum في أمر الشغل وقتها (الخطة لسه مش موجودة في تكنورا) - نفس مبدأ
عدم إرسال حقول لسه مفيش بيانات حقيقية ليها بدل ما نخمّن ونفشل.
"""
import asyncio
import json
import os
from contextlib import aclosing
from datetime import datetime, timezone
from pathlib import Path

import httpx

from maximo_client import MaximoClient
from teknora_client import TeknoraClient

# نقطة الاستكمال بتاعة الأنواع المقسمة على دفعات (أوامر الشغل) بتتحفظ على
# الديسك مش في الذاكرة - عشان تعيش بعد أي restart للكونتينر (docker-compose
# بيعمل volume على المجلد ده)
DATA_DIR = Path(os.environ.get("MIGRATOR_DATA_DIR", Path(__file__).resolve().parent.parent / "data"))
CHECKPOINT_FILE = DATA_DIR / "checkpoints.json"
DEFAULT_BATCH_SIZE = 100_000
SAVE_CONCURRENCY = 8

# العمالة الفعلية لأوامر الشغل - الاسم والحقول من عينة حقيقية
# (/api/maximo/sample/oslclabtrans): MXLABTRANS مش موجود في النسخة دي
LABTRANS_OS = "oslclabtrans"
LABTRANS_SELECT = ",".join(f"spi:{f}" for f in (
    "refwo", "siteid", "laborcode", "craft", "regularhrs", "payrate", "linecost",
    "startdate", "startdateentered", "finishdate", "finishdateentered", "transtype"))


def load_checkpoints() -> dict:
    try:
        return json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}


def save_checkpoint(cp_key: str, value) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    all_cp = load_checkpoints()
    if value is None:
        all_cp.pop(cp_key, None)
    else:
        all_cp[cp_key] = value
    tmp = CHECKPOINT_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(all_cp, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(CHECKPOINT_FILE)


FAILED_FILE = DATA_DIR / "failed.json"


def load_failed_store() -> dict:
    try:
        return json.loads(FAILED_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}


def save_failed(cp_key: str, entry: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    store = load_failed_store()
    store[cp_key] = entry
    tmp = FAILED_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(store, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(FAILED_FILE)


def failed_entry(cp_key: str) -> dict:
    """السجلات الفاشلة المستنية إعادة ({id: {ref, error}}) لنوع مقسم على
    دفعات. legacy_until = آخر ID اتنقل قبل ما تسجيل الفشل يتضاف: اللي فشل
    قبله مش متسجل، فإعادة الفاشل بتدوّر عليه في ماكسيمو (شوف _retry_failed)."""
    entry = load_failed_store().get(cp_key) or {}
    items = entry.setdefault("items", {})
    if "legacy_until" not in entry:
        cp = load_checkpoints().get(cp_key)
        entry["legacy_until"] = cp.get("last_id") if cp else None
        entry["legacy_done"] = entry["legacy_until"] is None
        for b in (cp or {}).get("maximo_unreadable_ids", []):
            items.setdefault(str(int(b)), {"ref": None, "error": "ماكسيمو مش قادر يرجّع السجل ده"})
    return entry


def checkpoint_key(maximo_base_url: str, type_key: str) -> str:
    # مفتاح مربوط بسيرفر ماكسيمو نفسه - عشان لو اتوصلت بسيرفر تاني متكملش
    # من نقطة استكمال بتاعة سيرفر مختلف
    return f"{maximo_base_url}|{type_key}"


def _describe_exc(e: Exception) -> str:
    """بعض الاستثناءات (زي انتهاء مهلة الاتصال) مالهاش نص وصفي في str(e) -
    فبنرجع اسم نوع الخطأ نفسه بدل رسالة فاضية غير مفيدة."""
    return str(e) or type(e).__name__


MIGRATION_ORDER = [
    "organizations", "persons", "crafts", "labor", "locations", "assets",
    "meters", "workorders", "meterreadings", "locationmeterreadings", "jobplans", "pm", "pmsequences",
]
# تسمية الأنواع (بالعربي والإنجليزي) مسؤولية الواجهة الأمامية بالكامل -
# الباك إند بيرجع بس المفاتيح التقنية (زي "assets")، عشان تبديل اللغة
# يكون قرار واجهة بحت من غير أي تكرار للترجمة في مكانين


def map_organization(o: dict) -> dict:
    # /organizations/save بيقرا "orgid" (من غير underscore) في المستوى
    # الأعلى بس - اتأكدنا من كود الـ endpoint نفسه. لو بعتنا "org_id" زي
    # باقي الأنواع، الطلب بيترفض بـ "Organization ID is required" رغم إن
    # القيمة موجودة فعليًا (اللي حصل فعليًا مع كل الـ 7 منظمات)
    return {
        "orgid": o.get("org_id"),
        "description": o.get("description") or o.get("org_id"),
        "itemsetid": "SET1",
        "companysetid": "SET1",
        "active": True,
        # /organizations/save بيبعت اللي إحنا بنبعته صراحةً حتى لو None -
        # وده بيلغي الـ default="EGP" بتاع العمود (SQLAlchemy مبيطبقش الـ
        # default غير لو العمود اتسابه خالص). النتيجة كانت NULL في قاعدة
        # البيانات، وشاشة عرض المنظمات في تكنورا بترفض القيمة دي بالكامل
        # (ResponseValidationError) وبتكراش لأي منظمة من غير عملة
        "basecurrency1": "EGP",
        "basecurrency2": "EGP",
        "sites": [
            {
                "site_id": s.get("site_id"),
                "description": s.get("description") or s.get("site_id"),
                "org_id": o.get("org_id"),
                "active": True,
            }
            for s in (o.get("sites") or []) if s.get("site_id")
        ],
    }


def map_location(m: dict) -> dict:
    # /locations/save بيرفض الطلب (422) لو "type" مش موجود - حقل إجباري
    # عندهم (اتأكدنا من رسالة الخطأ الفعلية)
    return {
        "location_id": m.get("location"),
        "description": m.get("description") or m.get("location"),
        "type": m.get("type") or "OPERATING",
        "status": m.get("status") or "OPERATING",
        "site_id": m.get("siteid"),
        "org_id": m.get("orgid"),
        "parent_id": m.get("parent") or None,
    }


def map_asset(m: dict) -> dict:
    return {
        "assetnum": m.get("assetnum"),
        "description": m.get("description") or m.get("assetnum"),
        "location_id": m.get("location"),
        "site_id": m.get("siteid"),
        "org_id": m.get("orgid"),
        "status": m.get("status") or "OPERATING",
        "priority": m.get("priority") or 3,
        "assettype": m.get("assettype"),
        "serial": m.get("serialnum"),
        "parent": m.get("parent"),
        "purchase_price": m.get("purchaseprice"),
        "replace_cost": m.get("replacecost"),
        "install_date": m.get("installdate"),
        "warranty_exp_date": m.get("warrantyexpdate"),
    }


def _sequence_rows(children: list) -> list:
    """صفوف السيكونس الفعلية من سجلات PMSEQUENCE_LOAD. السجل ممكن يكون
    سيكونس واحد مسطّح (فيه jpnum مباشرة)، أو سجل PM والسيكونس متداخل جوّاه
    تحت "pmsequence". المتداخل ليه الأولوية - لو السجل هو الـ PM نفسه، الـ
    jpnum اللي في مستواه الأول هو خطة الـ PM الأساسية مش سيكونس."""
    rows = []
    for c in children:
        nested = [n for k in ("pmsequence", "pmsequences") for n in (c.get(k) or []) if isinstance(n, dict)]
        if nested:
            rows.extend(s for s in (_strip_spi(n) for n in nested) if s.get("jpnum"))
        elif c.get("jpnum"):
            rows.append(c)
    return rows


def _find_sequence_list(d, depth: int = 0):
    """قائمة السيكونس جوه رد GET /pm/{pmnum} من تكنورا - الاسم بالظبط مش
    موثّق، فبندوّر على الأسماء المحتملة في المستوى الأول وجوه أي object
    متداخل مستوى واحد (زي {"pm": {...}, "sequences": [...]})."""
    if not isinstance(d, dict):
        return None
    for k in ("sequences", "pm_sequences", "pmsequences", "pmsequence", "sequence"):
        if isinstance(d.get(k), list):
            return d[k]
    if depth == 0:
        for v in d.values():
            found = _find_sequence_list(v, depth=1)
            if found is not None:
                return found
    return None


def _strip_spi(d: dict) -> dict:
    return {k.split(":", 1)[-1]: v for k, v in d.items() if isinstance(k, str)} if isinstance(d, dict) else {}


def map_jobplan(m: dict) -> dict:
    # /jobplans/save بيقبل مصفوفة "tasks" في نفس الطلب وبيعمل لها sync
    # كامل (مسح القديم وإضافة الجديد) - اتأكدنا من كود الـ endpoint نفسه.
    # الحقول جوه كل عنصر لازم تطابق أعمدة JPTask (task_sequence, description,
    # nested_jpnum, duration, meternum) - jpnum بيتضاف تلقائي من السيرفر
    # نفسه فمش لازم نبعته جوه كل task.
    # ملحوظة: labor/materials/services/tools ممكن تتبعت بنفس الطريقة، لكن
    # MXAPIJOBPLAN في النسخة دي من ماكسيمو بتعرض بس JOBTASK و JPASSETSPLIN
    # كـ Source Objects فرعية (اتأكدنا من شاشة Object Structures نفسها) -
    # يعني بيانات العمالة مش متاحة أصلاً من الـ Object Structure ده، محتاجة
    # تعديل إداري في ماكسيمو (إضافة JOBLABOR كـ child) لو مطلوبة لاحقًا
    tasks = []
    for t in (m.get("jobtask") or []):
        t = _strip_spi(t)
        tasks.append({
            "task_sequence": t.get("sequence") or t.get("tasknum"),
            "description": t.get("description"),
            "duration": t.get("duration"),
        })
    return {
        "jpnum": m.get("jpnum"),
        "description": m.get("description") or m.get("jpnum"),
        "status": m.get("status") or "ACTIVE",
        "org_id": m.get("orgid"),
        "site_id": m.get("siteid"),
        "tasks": tasks,
    }


def map_labor(m: dict) -> dict:
    # personid/craft_code بقوا مبعوتين فعليًا دلوقتي بما إن الأشخاص
    # والحرف بيتنقلوا قبل العمالة في الترتيب - لو الشخص/الحرفة المشار
    # ليهم لسه مش موجودين لأي سبب، هيفشل السجل ده بس ويظهر في التقرير
    return {
        "laborcode": m.get("laborcode"),
        "personid": m.get("personid") or None,
        "craft_code": m.get("craft") or None,
        "site_id": m.get("siteid"),
        "org_id": m.get("orgid"),
        "status": m.get("status") or "ACTIVE",
        # اسم الحقل ده تخمين لسه محتاج تأكيد (payrate/actrate بيختلف
        # حسب النسخة) - لو غلط هيظهر واضح كـ "فشل" مش هيوقف باقي السجلات
        "actual_rate": m.get("payrate") or m.get("actrate"),
    }


def map_pm(m: dict) -> dict:
    # /pm/save بيقبل "frequency" (dict أو list) وبيعمل sync كامل على جدول
    # PMFrequency - اتأكدنا من كود الـ endpoint نفسه. حقلين التكرار في
    # ماكسيمو (frequency/frequnit) موجودين كـ حقول مباشرة على سجل PM نفسه
    # (مش object فرعي منفصل)، وأسماؤهم مطابقة تمامًا لأعمدة PMFrequency
    frequency = None
    if m.get("frequency") is not None or m.get("frequnit"):
        frequency = {"frequency": m.get("frequency"), "frequnit": m.get("frequnit")}
    # تابة السيكونس: /pm/save بيقبل "sequences" وبيطابقها على أعمدة
    # PMSequence (jpnum, interval). MXAPIPM مبيرجعش PMSEQUENCE خالص، فبتتجاب
    # لوحدها من PMSEQUENCE_LOAD وبتتربط بكل PM قبل التحويل (شوف "attach" في
    # TYPE_SPECS) - لازم تتبعت مع الـ PM نفسه لأن /pm/save بيمسح السيكونس
    # القديم ويحط اللي جاي معاه
    sequences = [
        {"jpnum": s.get("jpnum"), "interval": s.get("interval")}
        for s in _sequence_rows(m.get("_sequences") or [])
    ]
    return {
        "pmnum": m.get("pmnum"),
        "description": m.get("description") or m.get("pmnum"),
        "status": m.get("status") or "ACTIVE",
        "org_id": m.get("orgid"),
        "site_id": m.get("siteid"),
        "assetnum": m.get("assetnum") or None,
        "location": m.get("location") or None,
        "asset_loc": m.get("location") or None,
        "worktype": m.get("worktype") or "PM",
        "priority": m.get("priority") or 3,
        "jpnum": m.get("jpnum") or None,
        "frequency": frequency,
        "sequences": sequences,
    }


def map_workorder(m: dict) -> dict:
    # عمدًا مبنبعتش jpnum هنا - أوامر الشغل بتتنقل قبل خطط العمل في الترتيب
    # المطلوب، فأي ربط بخطة لسه مش موجودة في تكنورا هيفشل بمخالفة مفتاح
    # خارجي. لو محتاج الربط ده لاحقًا، يحتاج "مرحلة تحديث" منفصلة بعد ما
    # خطط العمل تتنقل، مش جزء من النسخة الحالية.
    # أسماء حقول ماكسيمو هنا اتأكدنا منها من عينة MXAPIWO حقيقية
    # (/api/maximo/sample/mxapiwo): الأولوية اسمها wopriority (مفيش priority
    # خالص - فكل الأوامر كانت بتتحفظ بأولوية 3)، والجدولة schedstart وعمود
    # تكنورا المقابل اسمه scheddate
    #
    # الفرعيات (من عينة MXAPIWODETAIL وOSLCLABTRANS حقيقية)، وبتروح لـ
    # smart_sync في /workorder/save اللي بيشيل من كل صف id وwonum بس:
    # - tasks (woactivity): taskid بتاع ماكسيمو (10، 20...) بيروح wosequence -
    #   WOTask.taskid في تكنورا مفتاح أساسي للجدول كله، لو اتبعت كان هيتعارض
    #   بين أوامر الشغل (كل أمر فيه "تاسك 10")
    # - planned_labor (wplabor): من غير taskid - WOPLabor.taskid بيشاور على
    #   المفتاح الأساسي ده، فرقم تاسك ماكسيمو كان هيربط بتاسك أمر تاني
    # - actual_labor: من OSLCLABTRANS (بيتجاب لكل صفحة في _attach_actual_labor)
    tasks = [
        {"description": t.get("description"), "wosequence": t.get("taskid"),
         "status": t.get("status"), "estdur": t.get("estdur")}
        for t in (_strip_spi(x) for x in (m.get("woactivity") or []))
    ]
    planned_labor = [
        {"laborcode": p.get("laborcode") or None, "craft": p.get("craft") or None,
         "quantity": p.get("quantity"), "laborhrs": p.get("laborhrs"),
         "laborrate": p.get("rate"), "linecost": p.get("linecost")}
        for p in (_strip_spi(x) for x in (m.get("wplabor") or []))
    ]
    actual_labor = [
        {"laborcode": a.get("laborcode") or None,
         "startdate": a.get("startdate") or a.get("startdateentered"),
         "finishdate": a.get("finishdate") or a.get("finishdateentered"),
         "regularhrs": a.get("regularhrs"), "laborrate": a.get("payrate"), "linecost": a.get("linecost")}
        for a in (m.get("_actual_labor") or [])
    ]
    return {
        "tasks": tasks,
        "planned_labor": planned_labor,
        "actual_labor": actual_labor,
        "wonum": m.get("wonum"),
        "site_id": m.get("siteid"),
        "org_id": m.get("orgid"),
        "description": m.get("description") or m.get("wonum"),
        "wo_type": m.get("worktype") or "CM",
        "status": m.get("status") or "WAPPR",
        "assetnum": m.get("assetnum") or None,
        "location_id": m.get("location") or None,
        "priority": m.get("wopriority") if m.get("wopriority") is not None else 3,
        "parent_wo": m.get("parent") or None,
        "supervisor": m.get("supervisor") or None,
        "duration": m.get("estdur"),
        "targstartdate": m.get("targstartdate"),
        "targcompdate": m.get("targcompdate"),
        "scheddate": m.get("schedstart"),
        "schedfinish": m.get("schedfinish"),
        "actstart": m.get("actstart"),
        "actfinish": m.get("actfinish"),
        "reporteddate": m.get("reportdate") or m.get("reporteddate"),
        "reportedby": m.get("reportedby"),
    }


def map_person(m: dict) -> dict:
    return {
        "personid": m.get("personid"),
        "displayname": m.get("displayname") or m.get("personid"),
        "firstname": m.get("firstname"),
        "lastname": m.get("lastname"),
        "status": m.get("status") or "ACTIVE",
        "title": m.get("title"),
        "department": m.get("department"),
        "email": m.get("primaryemail") or m.get("email"),
        "phone": m.get("phone"),
    }


def map_craft(m: dict) -> dict:
    # رجّعنا site_id تاني - الفرضية إن الحرف مرتبطة بالمنظمة بس (من غير
    # site) كانت غلط، السكرين شوت الفعلي من ماكسيمو نفسه بيوضح إن الحرف
    # في النسخة دي فعليًا ليها Siteid حقيقي (1001, 1002...) مش فاضي. ظهور
    # "Global" وقت الاختبار الأول كان على الأرجح بسبب إن المنظمات والمواقع
    # كانت لسه فاشلة (باج orgid/org_id) وقتها، مش بسبب site_id نفسه
    return {
        "craft_code": m.get("craft"),
        "description": m.get("description") or m.get("craft"),
        "site_id": m.get("siteid"),
        "org_id": m.get("orgid"),
    }


def map_meter(m: dict) -> dict:
    return {
        "meter_num": m.get("metername"),
        "meter_name": m.get("metername"),
        "meter_type": m.get("metertype") or "GAUGE",
        "description": m.get("description") or m.get("metername"),
        "unit_of_measure": m.get("uom"),
    }


def map_meter_reading(m: dict) -> dict:
    return {
        "asset_num": m.get("assetnum"),
        "meter_num": m.get("metername"),
        "reading_value": m.get("reading") or m.get("newreading"),
        "reading_date": m.get("readingdate"),
    }


def map_location_meter_reading(m: dict) -> dict:
    return {
        "location_id": m.get("location"),
        "meter_num": m.get("metername"),
        "reading_value": m.get("lastreading") or m.get("reading") or m.get("newreading"),
        "reading_date": m.get("lastreadingdate") or m.get("readingdate"),
    }


# جدول واحد بيربط كل نوع بـ: اسم Object Structure في Maximo (None يعني
# دالة جلب خاصة، شوف organizations)، اسم الحقل المرجعي للتقرير (من بيانات
# Maximo الخام قبل التحويل)، دالة التحويل لحقول تكنورا، واسم دالة الحفظ
# المقابلة في TeknoraClient
TYPE_SPECS = {
    "organizations": {"os": None, "count_os": "mxorganization", "ref": "org_id", "map": map_organization, "save": "save_organization"},
    "persons": {"os": "person_load", "ref": "personid", "map": map_person, "save": "save_person"},
    "crafts": {"os": "mxcraft", "ref": "craft", "map": map_craft, "save": "save_craft"},
    "labor": {"os": "mxapilabor", "ref": "laborcode", "map": map_labor, "save": "save_labor"},
    "locations": {"os": "mxoperloc", "ref": "location", "map": map_location, "save": "save_location"},
    "assets": {"os": "mxasset", "ref": "assetnum", "map": map_asset, "save": "save_asset"},
    "meters": {"os": "oslcmeter", "ref": "metername", "map": map_meter, "save": "save_meter"},
    # batch_key: أوامر الشغل 22 مليون سجل - بتتنقل على دفعات مرتبة بالـ
    # workorderid، كل دفعة بتبدأ بعد آخر ID اتحفظ (مش بعد رقم صفحة، عشان
    # الصفحات العميقة في جدول بالحجم ده بطيئة جدًا في ماكسيمو)
    # select: الحقول اللي map_workorder بيستخدمها بس، مش "*" - أمر شغل واحد
    # فيه حقل ليه class مكسور على سيرفر ماكسيمو (BMXAA4183E) كان بيوقع
    # الصفحة كلها والدفعة كلها وراه (اللي حصل فعليًا بعد سجل 51,500)
    # MXAPIWODETAIL بدل MXAPIWO: فيه التاسكات (woactivity) والعمالة المخططة
    # (wplabor) جوه كل أمر. skip_if: صفوف التاسكات نفسها (istask) متتحفظش
    # كأوامر شغل - موجودة جوه الأمر الأب. select_fallback: لو ماكسيمو رفض
    # صيغة الـ select المتداخلة، بناخد كل الحقول (أبطأ بس شغال)
    "workorders": {"os": "mxapiwodetail", "ref": "wonum", "map": map_workorder, "save": "save_workorder",
                   "batch_key": "workorderid", "skip_if": "istask", "enrich": "_attach_actual_labor",
                   "select": ",".join([f"spi:{f}" for f in (
                       "workorderid", "wonum", "siteid", "orgid", "description", "worktype", "status",
                       "assetnum", "location", "wopriority", "parent", "supervisor", "estdur",
                       "targstartdate", "targcompdate", "schedstart", "schedfinish",
                       "actstart", "actfinish", "reportdate", "reportedby", "istask")] + [
                       "spi:woactivity{spi:taskid,spi:description,spi:status,spi:estdur}",
                       "spi:wplabor{spi:laborcode,spi:craft,spi:laborhrs,spi:quantity,spi:rate,spi:linecost}"]),
                   "select_fallback": "*"},
    "meterreadings": {"os": "mxmeterdata", "ref": "assetnum", "map": map_meter_reading, "save": "save_meter_reading"},
    "locationmeterreadings": {"os": "oslclocationmeter", "ref": "location", "map": map_location_meter_reading, "save": "save_location_meter_reading"},
    "jobplans": {"os": "mxapijobplan", "ref": "jpnum", "map": map_jobplan, "save": "save_jobplan", "inline": False},
    # attach: السيكونس بيتجاب من Object Structure منفصل ويتربط بكل PM بـ
    # (pmnum, siteid) قبل الحفظ
    "pm": {"os": "mxapipm", "ref": "pmnum", "map": map_pm, "save": "save_pm",
           "attach": {"os": "pmsequence_load", "key": "pmnum", "as": "_sequences", "label": "PM Sequences"}},
    # خطوة مستقلة للسيكونس بس عشان تتجرب لوحدها: بتبعت الـ PM كامل (مش
    # السيكونس لوحده، لأن /pm/save بيمسح التكرار ويصفّر الموقع لو مجوش في
    # الطلب) لكن للـ PMs اللي ليها سيكونس بس، وبعدين بتقرا كل PM من تكنورا
    # وتقارن العدد المتخزن بالمبعوت
    "pmsequences": {"os": "pmsequence_load", "ref": "pmnum", "custom": "_migrate_pm_sequences"},
}


class MigrationRun:
    def __init__(self, maximo: MaximoClient, teknora: TeknoraClient, selected_types: list,
                 batch_size: int = DEFAULT_BATCH_SIZE, mode: str = "migrate"):
        self.maximo = maximo
        self.teknora = teknora
        self.selected_types = set(selected_types)
        self.batch_size = batch_size
        self.mode = mode  # migrate | retry
        self._select_override = {}  # os -> select بديل لو ماكسيمو رفض الـ select الأصلي
        self.state = {
            "status": "idle",  # idle | running | done
            "current_type": None,
            "order": [t for t in MIGRATION_ORDER if t in self.selected_types],
            "types": {},
            "started_at": None,
            "finished_at": None,
            "fatal_error": None,
        }

    def _init_type(self, type_key: str, total: int):
        self.state["types"][type_key] = {
            "total": total,
            "done": 0,
            "success": 0,
            "failed": 0,
            "failures": [],
        }

    def _record_result(self, type_key: str, ref: str, error: str = None):
        st = self.state["types"][type_key]
        st["done"] += 1
        if error:
            st["failed"] += 1
            st["failures"].append({"ref": ref or "?", "error": error[:300]})
        else:
            st["success"] += 1

    async def run(self):
        self.state["status"] = "running"
        self.state["started_at"] = datetime.now(timezone.utc).isoformat()
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                for type_key in self.state["order"]:
                    self.state["current_type"] = type_key
                    if self.mode == "retry":
                        await self._retry_failed(type_key, client)
                    elif TYPE_SPECS[type_key].get("custom"):
                        await getattr(self, TYPE_SPECS[type_key]["custom"])(type_key, client)
                    elif TYPE_SPECS[type_key].get("batch_key"):
                        await self._migrate_batched(type_key, client)
                    else:
                        await self._migrate_type(type_key, client)
        except Exception as e:
            self.state["fatal_error"] = _describe_exc(e)
        finally:
            self.state["current_type"] = None
            self.state["status"] = "done"
            self.state["finished_at"] = datetime.now(timezone.utc).isoformat()

    async def _save_record(self, type_key: str, spec: dict, save_fn, client: httpx.AsyncClient, r: dict):
        """None لو اتحفظ، أو نص الخطأ لو فشل."""
        # بعض الحقول (زي "location" في oslclocationmeter) بترجع كمرجع
        # {"rdf:resource": "..."} لسجل تاني بدل القيمة الفعلية - لازم نتبعها
        # ونستبدلها قبل التحويل، وإلا هترسل كـ dict لتكنورا وترجع 422
        for key in ("location", "assetnum", "asset"):
            val = r.get(key)
            if isinstance(val, dict) and "rdf:resource" in val:
                resolved = await self.maximo.resolve_ref(val)
                r[key] = resolved.get(key) or resolved.get("location") or resolved.get("assetnum")

        ref = r.get(spec["ref"])
        try:
            await save_fn(client, spec["map"](r))
            self._record_result(type_key, ref)
            return None
        except Exception as e:
            err = _describe_exc(e)
            if not ref:
                # مفيش قيمة للحقل المرجعي - غالبًا اسم الحقل في map_* مش مطابق
                # للاسم الحقيقي في رد Maximo، فبنضيف مفاتيح السجل الخام للتشخيص
                err += f" | مفاتيح Maximo المتاحة: {list(r.keys())}"
            self._record_result(type_key, ref, err)
            return err

    async def _migrate_type(self, type_key: str, client: httpx.AsyncClient):
        spec = TYPE_SPECS[type_key]
        timeout_s = 1800
        try:
            # مهلة قصوى للجلب الأولي عشان أي تعليق في الاتصال بماكسيمو يفشّل
            # النوع ده بس بدل ما يوقف النقل كله من غير رسالة. 30 دقيقة لأن
            # الأصول (~18 ألف سجل) بتاخد وقت فعلي وهي شغالة عادي
            if spec["os"] is None:
                records = await asyncio.wait_for(self.maximo.get_organizations_with_sites(), timeout=timeout_s)
            else:
                records = await asyncio.wait_for(
                    self.maximo.query_all(spec["os"], inline=spec.get("inline", True)), timeout=timeout_s)
        except Exception as e:
            if isinstance(e, asyncio.TimeoutError):
                reason = f"الجلب من Maximo عدّى {timeout_s // 60} دقيقة ولسه مخلصش"
            else:
                reason = _describe_exc(e)
            self._init_type(type_key, 0)
            self.state["types"][type_key]["failures"].append({
                "ref": "-", "error": f"تعذر جلب البيانات من Maximo: {reason}"
            })
            return

        self._init_type(type_key, len(records))
        if spec.get("attach"):
            await self._attach_children(type_key, spec["attach"], records)
        save_fn = getattr(self.teknora, spec["save"])
        for r in records:
            await self._save_record(type_key, spec, save_fn, client, r)

    async def _migrate_pm_sequences(self, type_key: str, client: httpx.AsyncClient):
        pm_spec = TYPE_SPECS["pm"]
        attach = pm_spec["attach"]
        self._init_type(type_key, 0)
        st = self.state["types"][type_key]
        try:
            children = await asyncio.wait_for(self.maximo.query_all(attach["os"]), timeout=1800)
            pms = await asyncio.wait_for(
                self.maximo.query_all(pm_spec["os"], inline=pm_spec.get("inline", True)), timeout=1800)
        except Exception as e:
            reason = "عدّى 30 دقيقة" if isinstance(e, asyncio.TimeoutError) else _describe_exc(e)
            st["failures"].append({"ref": "-", "error": f"تعذر جلب البيانات من Maximo: {reason}"})
            return

        await self._attach_children(type_key, attach, pms, children=children)
        targets = [p for p in pms if p.get(attach["as"])]
        st["total"] = len(targets)
        st["summary"] = {"sequences_in_maximo": len(children), "pms_in_maximo": len(pms),
                         "pms_with_sequences": len(targets)}

        unverifiable_noted = False
        unreadable_slash = []
        for p in targets:
            ref = p.get("pmnum")
            payload = map_pm(p)
            sent = len(payload["sequences"])
            if not sent:
                # ماكسيمو رجّع سجلات سيكونس للـ PM ده بس ملقيناش فيها jpnum -
                # الإرسال بقائمة فاضية كان بيعدّي "ناجح" وتكنورا بيمسح أي
                # سيكونس موجود (اللي حصل فعليًا: ولا صف اتخزن)، فمنبعتش خالص
                c = p[attach["as"]][0]
                nested = {k: sorted(_strip_spi(c[k][0]).keys())[:20] for k in c
                          if isinstance(c.get(k), list) and c[k] and isinstance(c[k][0], dict)}
                self._record_result(type_key, ref,
                                    f"ماكسيمو رجّع {len(p[attach['as']])} سجل سيكونس للـ PM ده بس ملقيناش فيهم jpnum"
                                    f" - مفاتيح السجل: {sorted(c.keys())[:30]} - القوايم المتداخلة: {nested}")
                continue
            try:
                await self.teknora.save_pm(client, payload)
            except Exception as e:
                self._record_result(type_key, ref, _describe_exc(e))
                continue
            try:
                stored_pm = await self.teknora.get_pm(client, ref)
            except Exception as e:
                if "/" in (ref or "") and "HTTP 404" in str(e):
                    # GET /pm/{pmnum} في تكنورا مش بيطابق أي رقم فيه "/" (الـ
                    # path بيتقسم على الـ "/" فمفيش route يطابقه) - الحفظ اتقبل،
                    # بس مفيش طريقة نقرا بيها السجل نتأكد
                    unreadable_slash.append(ref)
                    self._record_result(type_key, ref)
                else:
                    self._record_result(type_key, ref, f"الطلب اتقبل بس مقدرناش نقرا الـ PM من تكنورا نتأكد: {_describe_exc(e)}")
                continue
            stored = _find_sequence_list(stored_pm)
            if stored is None:
                # شكل رد GET /pm/{pmnum} مش معروف مسبقًا - لو ملقيناش قائمة
                # سيكونس فيه، بنقول ده مرة واحدة بمفاتيح الرد الحقيقية بدل
                # ما نحكم على السجل بتخمين
                if not unverifiable_noted:
                    keys = sorted(stored_pm.keys())[:30] if isinstance(stored_pm, dict) else type(stored_pm).__name__
                    st["failures"].append({"ref": ref, "error": f"اتحفظ، بس رد تكنورا لـ GET /pm/{{pmnum}} مفيهوش قائمة سيكونس نقارن بيها - مفاتيح الرد: {keys}"})
                    unverifiable_noted = True
                self._record_result(type_key, ref)
            elif len(stored) != sent:
                self._record_result(type_key, ref, f"اتبعت {sent} سيكونس والطلب اتقبل، بس تكنورا متخزن فيه {len(stored)}")
            else:
                self._record_result(type_key, ref)

        if unreadable_slash:
            st["failures"].append({
                "ref": unreadable_slash[0],
                "error": (f"{len(unreadable_slash)} PM رقمهم فيه \"/\": اتحفظوا، بس GET /pm/{{pmnum}} في تكنورا "
                          f"بيرجع 404 لأي رقم فيه \"/\"، فمقدرناش نتأكد من السيكونس بتاعهم "
                          f"(وغالبًا شاشة تكنورا نفسها مش هتعرف تفتحهم) - أمثلة: {unreadable_slash[:5]}"),
            })

    async def _attach_children(self, type_key: str, attach: dict, records: list, children: list = None):
        """بيجيب سجلات فرعية من Object Structure منفصل (زي PMSEQUENCE_LOAD)
        ويربطها بكل سجل أب بـ (key, siteid). siteid جزء من المفتاح لأن
        pmnum في ماكسيمو مميز جوه الـ site بس، مش على مستوى النظام كله."""
        key, target = attach["key"], attach["as"]
        if children is None:
            try:
                children = await asyncio.wait_for(self.maximo.query_all(attach["os"]), timeout=1800)
            except Exception as e:
                reason = "عدّى 30 دقيقة" if isinstance(e, asyncio.TimeoutError) else _describe_exc(e)
                # الأب بيتحفظ عادي من غير الفرعيات بدل ما النوع كله يقف
                self.state["types"][type_key]["failures"].append({
                    "ref": "-", "error": f"تعذر جلب {attach['label']} من Maximo ({attach['os']}): {reason}"
                })
                return

        # الربط بـ (key, siteid) لو السجلات الفرعية راجعة بـ siteid، وإلا بـ
        # key لوحده - لو الـ Object Structure مش بيرجّع siteid، الربط بالزوج
        # كان هيفشل كله بصمت والقائمة تيجي فاضية (اللي حصل فعليًا)
        use_site = any(c.get("siteid") for c in children)
        groups = {}
        for c in children:
            gk = (c.get(key), c.get("siteid")) if use_site else c.get(key)
            groups.setdefault(gk, []).append(c)

        matched = 0
        for r in records:
            gk = (r.get(key), r.get("siteid")) if use_site else r.get(key)
            r[target] = groups.get(gk, [])
            matched += bool(r[target])

        # مش فشل سجل بعينه، بس لازم يبان في التقرير - قائمة فاضية من غير أي
        # رسالة هي بالظبط اللي خلانا منعرفش إن السيكونس مبيوصلش
        label = f"{attach['label']} ({attach['os']})"
        if not children:
            note = f"{label}: ماكسيمو رجّع 0 سجل - مفيش حاجة تتربط"
        elif not matched:
            note = (f"{label}: اتجاب {len(children)} سجل بس ولا واحد اتربط بأي سجل أب بالمفتاح '{key}'"
                    f" - مفاتيح السجل الفرعي: {sorted(children[0].keys())[:25]}")
        else:
            note = None
        if note:
            self.state["types"][type_key]["failures"].append({"ref": "-", "error": note})
        print(f"[migration] {attach['os']}: {len(children)} children, attached to {matched} of {len(records)} records")

    async def _fetch_page_with_retry(self, spec: dict, where: str, key: str, attempts: int = 3,
                                     select: str = None, page_size: int = 500) -> list:
        """3 محاولات بانتظار متزايد (5ث، 15ث) - عطل عابر في صفحة واحدة
        مينهيش الدفعة كلها. لو الـ 3 فشلوا الخطأ بيطلع لفوق."""
        for attempt in range(attempts):
            try:
                return await self.maximo.fetch_first_page(
                    spec["os"], where=where, order_by=f"+spi:{key}", page_size=page_size,
                    inline=spec.get("inline", True),
                    select=select or self._select_override.get(spec["os"], spec.get("select")))
            except Exception:
                if attempt == attempts - 1:
                    raise
                await asyncio.sleep(5 * 3 ** attempt)

    async def _fetch_next_page(self, spec: dict, key: str, last_id) -> tuple:
        """(السجلات، [(id، سبب) للسجلات اللي ماكسيمو مش قادر يرجّعها]).
        لو الصفحة فشلت حتى بعد المحاولات، بنجيب الـ IDs بس (حقل واحد سليم)
        ونقسم الصفحة نصين نصين لحد ما نعزل السجل (أو السجلات) المكسورة -
        سجل واحد مينفعش يوقف الـ 22 مليون كلهم."""
        where = f"spi:{key}>{last_id}" if last_id is not None else None
        try:
            return await self._fetch_page_with_retry(spec, where, key), []
        except Exception as e:
            page_error = e

        id_recs = await self._fetch_page_with_retry(spec, where, key, select=f"spi:{key}")
        ids = [r.get(key) for r in id_recs if isinstance(r.get(key), (int, float))]
        if not ids:
            raise page_error
        return await self._fetch_ids_bisect(spec, key, sorted(ids))

    async def _fetch_ids_bisect(self, spec: dict, key: str, ids: list) -> tuple:
        where = f"spi:{key}>={ids[0]} and spi:{key}<={ids[-1]}"
        try:
            recs = await self._fetch_page_with_retry(spec, where, key, page_size=len(ids),
                                                     attempts=2 if len(ids) == 1 else 1)
            return recs, []
        except Exception as e:
            if len(ids) == 1:
                return [], [(ids[0], _describe_exc(e))]
            mid = len(ids) // 2
            left, bad_left = await self._fetch_ids_bisect(spec, key, ids[:mid])
            right, bad_right = await self._fetch_ids_bisect(spec, key, ids[mid:])
            return left + right, bad_left + bad_right

    async def _migrate_batched(self, type_key: str, client: httpx.AsyncClient):
        """دفعة واحدة (batch_size سجل) بتبدأ بعد آخر ID اتحفظ في المرة اللي
        فاتت. الجلب والحفظ صفحة بصفحة (مش تحميل الدفعة كلها في الذاكرة)،
        ونقطة الاستكمال بتتحفظ على الديسك بعد كل صفحة - لو الاتصال وقع في
        النص، التشغيلة الجاية بتكمل من آخر صفحة خلصت."""
        spec = TYPE_SPECS[type_key]
        key = spec["batch_key"]
        cp_key = checkpoint_key(self.maximo.base_url, type_key)
        cp = load_checkpoints().get(cp_key) or {"last_id": None, "migrated": 0, "failed": 0, "batches": 0}
        start_after = cp.get("last_id")
        # لازم يتقرا قبل ما نقطة الاستكمال تتحرك، عشان legacy_until يتسجل
        # على آخر ID اتنقل قبل تسجيل الفشل
        fail_entry = failed_entry(cp_key)
        save_failed(cp_key, fail_entry)
        failed_items = fail_entry["items"]

        self._init_type(type_key, self.batch_size)
        st = self.state["types"][type_key]
        st["batch"] = {"size": self.batch_size, "start_after": start_after, "last_id": start_after,
                       "migrated_before": cp.get("migrated", 0), "finished_all": False, "skipped": 0}

        save_fn = getattr(self.teknora, spec["save"])
        sem = asyncio.Semaphore(SAVE_CONCURRENCY)

        async def save_limited(r):
            async with sem:
                return await self._save_record(type_key, spec, save_fn, client, r)

        fetched = 0
        last_id = start_after
        try:
            await self._probe_select(spec, type_key)
            while fetched < self.batch_size:
                # كل صفحة استعلام جديد "أول 500 بعد آخر ID" (keyset) - مفيش
                # صفحات بعيدة خالص، فالطلب رقم 400 بنفس سرعة الأول
                page, bad = await self._fetch_next_page(spec, key, last_id)
                if not page and not bad:
                    st["batch"]["finished_all"] = True
                    break

                ids = [r.get(key) for r in page]
                if any(not isinstance(i, (int, float)) for i in ids):
                    raise Exception(f"الحقل '{key}' مش راجع في بيانات ماكسيمو - مينفعش نقسم على دفعات من غيره")
                if ids != sorted(ids):
                    # لو ماكسيمو تجاهل الترتيب، "بعد آخر ID" هيعدّي سجلات
                    # بصمت - نوقف بوضوح أحسن من فقد بيانات
                    raise Exception(f"ماكسيمو رجّع السجلات مش مترتبة بالـ {key} - وقفنا عشان منعدّيش سجلات")

                for bad_id, reason in bad:
                    err = f"ماكسيمو مش قادر يرجّع السجل ده (اتعزل واتعدّى): {reason}"
                    self._record_result(type_key, f"{key}={int(bad_id)}", err)
                    failed_items[str(int(bad_id))] = {"ref": None, "error": err[:300]}
                to_save = self._without_skipped(spec, page)
                st["batch"]["skipped"] += len(page) - len(to_save)
                if spec.get("enrich") and to_save:
                    # لو جلب الفرعيات فشل بيرمي ويوقف الدفعة من غير ما نقطة
                    # الاستكمال تتحرك - أحسن من إن الأوامر تتحفظ ناقصة بصمت
                    await getattr(self, spec["enrich"])(spec, to_save)
                errs = await asyncio.gather(*[save_limited(r) for r in to_save])
                ok = [e is None for e in errs]
                for r, err in zip(to_save, errs):
                    rid = str(int(r[key]))
                    if err is None:
                        failed_items.pop(rid, None)
                    else:
                        failed_items[rid] = {"ref": r.get(spec["ref"]), "error": err[:300]}
                fetched += len(page) + len(bad)
                if to_save and not any(ok):
                    # صفحة كاملة فشلت = غالبًا مشكلة عامة (تكنورا واقع، توكن...)
                    # مش مشكلة بيانات - منحركش نقطة الاستكمال عشان منعدّيش
                    # 500 سجل من غير ما يتنقلوا. ومنسجلهمش فاشلين كمان: هيتعادوا
                    # لوحدهم من نقطة الاستكمال في التشغيلة الجاية
                    for r in to_save:
                        failed_items.pop(str(int(r[key])), None)
                    st["failures"].append({"ref": "-", "error": "كل سجلات صفحة كاملة فشلت، فوقفنا الدفعة من غير ما نحرّك نقطة الاستكمال - شوف أسباب الفشل اللي فوق"})
                    return

                last_id = int(max(ids + [b for b, _ in bad]))
                cp = {
                    "last_id": last_id,
                    "migrated": cp.get("migrated", 0) + sum(ok),
                    "failed": cp.get("failed", 0) + (len(ok) - sum(ok)) + len(bad),
                    "batches": cp.get("batches", 0),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
                # الفاشل بيتحفظ قبل نقطة الاستكمال: لو الكونتينر وقع بينهم،
                # أسوأ حاجة إن الصفحة تتعاد، مش إن سجلات فاشلة تضيع
                save_failed(cp_key, fail_entry)
                save_checkpoint(cp_key, cp)
                st["batch"]["last_id"] = last_id
            cp["batches"] = cp.get("batches", 0) + 1
            if fetched:
                save_checkpoint(cp_key, cp)
        except Exception as e:
            st["failures"].append({"ref": "-", "error": f"تعذر جلب البيانات من Maximo: {_describe_exc(e)}"})
        finally:
            st["total"] = st["done"]

    async def _retry_failed(self, type_key: str, client: httpx.AsyncClient):
        """بيعيد السجلات الفاشلة بالظبط بأرقامها (مش مدى أرقام كامل - أمر
        شغل CLOSE اتحفظ قبل كده تكنورا بيرفض يتعدّل). أول مرة بس: بيضيف
        أوامر COMP اللي اتنقلت قبل تسجيل الفشل، لأن كلها فشلت من غير استثناء
        (update_asset_costs في تكنورا كان بيقع مع أي COMP)."""
        spec = TYPE_SPECS[type_key]
        key = spec["batch_key"]
        cp_key = checkpoint_key(self.maximo.base_url, type_key)
        entry = failed_entry(cp_key)
        items = entry["items"]
        self._init_type(type_key, 0)
        st = self.state["types"][type_key]
        st["retry"] = {"legacy_added": 0}

        try:
            if not entry.get("legacy_done") and entry.get("legacy_until") is not None:
                for i in await self._scan_ids(spec, key, 'spi:status="COMP"', upto=entry["legacy_until"]):
                    if str(i) not in items:
                        items[str(i)] = {"ref": None, "error": "أمر COMP من دفعة قبل تسجيل الفشل"}
                        st["retry"]["legacy_added"] += 1
                entry["legacy_done"] = True
                save_failed(cp_key, entry)
        except Exception as e:
            st["failures"].append({"ref": "-", "error": f"تعذر البحث عن أوامر COMP القديمة في Maximo: {_describe_exc(e)}"})
            return

        ids = sorted(int(i) for i in items)
        st["total"] = len(ids)
        st["retry"]["attempted"] = len(ids)
        save_fn = getattr(self.teknora, spec["save"])
        sem = asyncio.Semaphore(SAVE_CONCURRENCY)

        async def save_limited(r):
            async with sem:
                return await self._save_record(type_key, spec, save_fn, client, r)

        try:
            await self._probe_select(spec, type_key)
            for n in range(0, len(ids), 100):
                chunk = ids[n:n + 100]
                recs, unfetched = await self._fetch_exact_ids(spec, key, chunk)
                for i, reason in unfetched:
                    self._record_result(type_key, f"{key}={i}", reason)
                    items[str(i)] = {"ref": items.get(str(i), {}).get("ref"), "error": reason[:300]}
                kept = self._without_skipped(spec, recs)
                for r in recs:
                    if r not in kept:
                        items.pop(str(int(r[key])), None)
                recs = kept
                if spec.get("enrich") and recs:
                    await getattr(self, spec["enrich"])(spec, recs)
                errs = await asyncio.gather(*[save_limited(r) for r in recs])
                for r, err in zip(recs, errs):
                    rid = str(int(r[key]))
                    if err is None:
                        items.pop(rid, None)
                    else:
                        items[rid] = {"ref": r.get(spec["ref"]), "error": err[:300]}
                save_failed(cp_key, entry)
        except Exception as e:
            st["failures"].append({"ref": "-", "error": f"تعذر جلب البيانات من Maximo: {_describe_exc(e)}"})
        finally:
            st["retry"]["still_failing"] = len(items)
            st["total"] = st["done"]

    @staticmethod
    def _without_skipped(spec: dict, records: list) -> list:
        skip = spec.get("skip_if")
        return [r for r in records if not (skip and r.get(skip))] if skip else list(records)

    async def _probe_select(self, spec: dict, type_key: str):
        """بيجرّب الـ select على سجل واحد قبل الدفعة. لو ماكسيمو رفضه (صيغة
        القوايم المتداخلة مش مدعومة في كل النسخ)، بنكمل بـ select_fallback
        ونقول ده في التقرير بدل ما كل صفحة تفشل."""
        sel, fallback = spec.get("select"), spec.get("select_fallback")
        if not sel or not fallback or spec["os"] in self._select_override:
            return
        try:
            await self.maximo.fetch_first_page(spec["os"], page_size=1, inline=spec.get("inline", True), select=sel)
        except Exception as e:
            self._select_override[spec["os"]] = fallback
            self.state["types"][type_key]["failures"].append({
                "ref": "-", "error": f"ماكسيمو رفض الحقول المحددة (select)، فكمّلنا بكل الحقول بدلها - أبطأ بس شغال: {_describe_exc(e)[:200]}"
            })

    async def _attach_actual_labor(self, spec: dict, parents: list):
        """العمالة الفعلية من OSLCLABTRANS لكل أمر في الصفحة، بما فيها العمالة
        المتسجلة على تاسكات الأمر (refwo = رقم التاسك مش الأب - عينة حقيقية
        فيها enteredastask: true). الأوامر وتاسكاتها بتتربط بالموقع (siteid)
        لأن رقم أمر الشغل في ماكسيمو مميز جوه الموقع بس."""
        for p in parents:
            p["_actual_labor"] = []
        by_site = {}
        for p in parents:
            if p.get("wonum"):
                by_site.setdefault(p.get("siteid"), []).append(p)

        for site, ps in by_site.items():
            site_cond = f'spi:siteid={json.dumps(site)} and ' if site else ""
            owner = {p["wonum"]: p for p in ps}
            wonums = list(owner)
            for n in range(0, len(wonums), 100):
                chunk = ", ".join(json.dumps(w) for w in wonums[n:n + 100])
                tasks = await self.maximo.query_all(
                    spec["os"], where=f"{site_cond}spi:parent in [{chunk}]",
                    select="spi:wonum,spi:parent,spi:siteid")
                for t in tasks:
                    if t.get("wonum") and t.get("parent") in owner:
                        owner.setdefault(t["wonum"], owner[t["parent"]])

            refs = list(owner)
            for n in range(0, len(refs), 100):
                chunk = ", ".join(json.dumps(w) for w in refs[n:n + 100])
                rows = await self.maximo.query_all(
                    LABTRANS_OS, where=f"{site_cond}spi:refwo in [{chunk}]", select=LABTRANS_SELECT)
                for row in rows:
                    parent = owner.get(row.get("refwo"))
                    if parent is not None:
                        parent["_actual_labor"].append(row)

    async def _scan_ids(self, spec: dict, key: str, condition: str, upto: int) -> list:
        """كل الـ IDs اللي بتحقق شرط لحد upto - keyset، الـ ID بس (حقل واحد
        سليم، نفس اللي بنعزل بيه السجلات المكسورة)."""
        ids, last = [], None
        while True:
            where = f"{condition} and spi:{key}<={upto}"
            if last is not None:
                where += f" and spi:{key}>{last}"
            page = await self._fetch_page_with_retry(spec, where, key, select=f"spi:{key}")
            page_ids = [int(r[key]) for r in page if isinstance(r.get(key), (int, float))]
            if not page_ids:
                return ids
            ids.extend(page_ids)
            last = max(page_ids)

    async def _fetch_exact_ids(self, spec: dict, key: str, ids: list) -> tuple:
        """(السجلات، [(id، سبب)]) للأرقام دي بالظبط. "in" الأول؛ لو فشل أو
        رجّع ولا سجل، واحد واحد - عشان "in" متفهمش غلط يخلي أرقام موجودة
        تتقال إنها مش موجودة."""
        wanted = set(ids)
        recs = []
        try:
            got = await self._fetch_page_with_retry(spec, f"spi:{key} in [{','.join(map(str, ids))}]", key,
                                                    page_size=len(ids), attempts=2)
            recs = [r for r in got if isinstance(r.get(key), (int, float)) and int(r[key]) in wanted]
        except Exception:
            recs = []

        unfetched = []
        if not recs:
            for i in ids:
                try:
                    got = await self._fetch_page_with_retry(spec, f"spi:{key}={i}", key, page_size=1, attempts=2)
                    recs.extend(r for r in got if isinstance(r.get(key), (int, float)) and int(r[key]) == i)
                except Exception as e:
                    unfetched.append((i, f"ماكسيمو مش قادر يرجّع السجل ده: {_describe_exc(e)}"))

        found = {int(r[key]) for r in recs}
        failed_ids = {i for i, _ in unfetched}
        unfetched += [(i, "السجل مش موجود في ماكسيمو دلوقتي") for i in ids if i not in found and i not in failed_ids]
        return recs, unfetched

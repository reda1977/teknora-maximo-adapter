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
    "meters", "workorders", "meterreadings", "locationmeterreadings", "jobplans", "pm",
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
        for s in (m.get("_sequences") or [])
        if s.get("jpnum")
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
    return {
        "wonum": m.get("wonum"),
        "site_id": m.get("siteid"),
        "org_id": m.get("orgid"),
        "description": m.get("description") or m.get("wonum"),
        "wo_type": m.get("worktype") or "CM",
        "status": m.get("status") or "WAPPR",
        "assetnum": m.get("assetnum") or None,
        "location_id": m.get("location") or None,
        "priority": m.get("priority") or 3,
        "targstartdate": m.get("targstartdate"),
        "targcompdate": m.get("targcompdate"),
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
    "workorders": {"os": "mxapiwo", "ref": "wonum", "map": map_workorder, "save": "save_workorder",
                   "batch_key": "workorderid"},
    "meterreadings": {"os": "mxmeterdata", "ref": "assetnum", "map": map_meter_reading, "save": "save_meter_reading"},
    "locationmeterreadings": {"os": "oslclocationmeter", "ref": "location", "map": map_location_meter_reading, "save": "save_location_meter_reading"},
    "jobplans": {"os": "mxapijobplan", "ref": "jpnum", "map": map_jobplan, "save": "save_jobplan", "inline": False},
    # attach: السيكونس بيتجاب من Object Structure منفصل ويتربط بكل PM بـ
    # (pmnum, siteid) قبل الحفظ
    "pm": {"os": "mxapipm", "ref": "pmnum", "map": map_pm, "save": "save_pm",
           "attach": {"os": "pmsequence_load", "key": "pmnum", "as": "_sequences", "label": "PM Sequences"}},
}


class MigrationRun:
    def __init__(self, maximo: MaximoClient, teknora: TeknoraClient, selected_types: list,
                 batch_size: int = DEFAULT_BATCH_SIZE):
        self.maximo = maximo
        self.teknora = teknora
        self.selected_types = set(selected_types)
        self.batch_size = batch_size
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
                    if TYPE_SPECS[type_key].get("batch_key"):
                        await self._migrate_batched(type_key, client)
                    else:
                        await self._migrate_type(type_key, client)
        except Exception as e:
            self.state["fatal_error"] = _describe_exc(e)
        finally:
            self.state["current_type"] = None
            self.state["status"] = "done"
            self.state["finished_at"] = datetime.now(timezone.utc).isoformat()

    async def _save_record(self, type_key: str, spec: dict, save_fn, client: httpx.AsyncClient, r: dict) -> bool:
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
            return True
        except Exception as e:
            err = _describe_exc(e)
            if not ref:
                # مفيش قيمة للحقل المرجعي - غالبًا اسم الحقل في map_* مش مطابق
                # للاسم الحقيقي في رد Maximo، فبنضيف مفاتيح السجل الخام للتشخيص
                err += f" | مفاتيح Maximo المتاحة: {list(r.keys())}"
            self._record_result(type_key, ref, err)
            return False

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

    async def _attach_children(self, type_key: str, attach: dict, records: list):
        """بيجيب سجلات فرعية من Object Structure منفصل (زي PMSEQUENCE_LOAD)
        ويربطها بكل سجل أب بـ (key, siteid). siteid جزء من المفتاح لأن
        pmnum في ماكسيمو مميز جوه الـ site بس، مش على مستوى النظام كله."""
        key, target = attach["key"], attach["as"]
        try:
            children = await asyncio.wait_for(self.maximo.query_all(attach["os"]), timeout=1800)
        except Exception as e:
            reason = "عدّى 30 دقيقة" if isinstance(e, asyncio.TimeoutError) else _describe_exc(e)
            # الأب بيتحفظ عادي من غير الفرعيات بدل ما النوع كله يقف
            self.state["types"][type_key]["failures"].append({
                "ref": "-", "error": f"تعذر جلب {attach['label']} من Maximo ({attach['os']}): {reason}"
            })
            return

        groups = {}
        for c in children:
            groups.setdefault((c.get(key), c.get("siteid")), []).append(c)
        for r in records:
            r[target] = groups.get((r.get(key), r.get("siteid")), [])

    async def _fetch_page_with_retry(self, spec: dict, where: str, key: str, attempts: int = 3) -> list:
        """3 محاولات بانتظار متزايد (5ث، 15ث) - عطل عابر في صفحة واحدة
        مينهيش الدفعة كلها. لو الـ 3 فشلوا الخطأ بيطلع في التقرير."""
        for attempt in range(attempts):
            try:
                return await self.maximo.fetch_first_page(spec["os"], where=where, order_by=f"+spi:{key}",
                                                          inline=spec.get("inline", True))
            except Exception:
                if attempt == attempts - 1:
                    raise
                await asyncio.sleep(5 * 3 ** attempt)

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

        self._init_type(type_key, self.batch_size)
        st = self.state["types"][type_key]
        st["batch"] = {"size": self.batch_size, "start_after": start_after, "last_id": start_after,
                       "migrated_before": cp.get("migrated", 0), "finished_all": False}

        save_fn = getattr(self.teknora, spec["save"])
        sem = asyncio.Semaphore(SAVE_CONCURRENCY)

        async def save_limited(r):
            async with sem:
                return await self._save_record(type_key, spec, save_fn, client, r)

        fetched = 0
        last_id = start_after
        try:
            while fetched < self.batch_size:
                # كل صفحة استعلام جديد "أول 500 بعد آخر ID" (keyset) - مفيش
                # صفحات بعيدة خالص، فالطلب رقم 400 بنفس سرعة الأول
                where = f"spi:{key}>{last_id}" if last_id is not None else None
                page = await self._fetch_page_with_retry(spec, where, key)
                page = page[: self.batch_size - fetched]
                if not page:
                    st["batch"]["finished_all"] = True
                    break

                ids = [r.get(key) for r in page]
                if any(not isinstance(i, (int, float)) for i in ids):
                    raise Exception(f"الحقل '{key}' مش راجع في بيانات ماكسيمو - مينفعش نقسم على دفعات من غيره")
                if ids != sorted(ids):
                    # لو ماكسيمو تجاهل الترتيب، "بعد آخر ID" هيعدّي سجلات
                    # بصمت - نوقف بوضوح أحسن من فقد بيانات
                    raise Exception(f"ماكسيمو رجّع السجلات مش مترتبة بالـ {key} - وقفنا عشان منعدّيش سجلات")

                ok = await asyncio.gather(*[save_limited(r) for r in page])
                fetched += len(page)
                if not any(ok):
                    # صفحة كاملة فشلت = غالبًا مشكلة عامة (تكنورا واقع، توكن...)
                    # مش مشكلة بيانات - منحركش نقطة الاستكمال عشان منعدّيش
                    # 500 سجل من غير ما يتنقلوا
                    st["failures"].append({"ref": "-", "error": "كل سجلات صفحة كاملة فشلت، فوقفنا الدفعة من غير ما نحرّك نقطة الاستكمال - شوف أسباب الفشل اللي فوق"})
                    return

                last_id = int(max(ids))
                cp = {
                    "last_id": last_id,
                    "migrated": cp.get("migrated", 0) + sum(ok),
                    "failed": cp.get("failed", 0) + (len(ok) - sum(ok)),
                    "batches": cp.get("batches", 0),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
                save_checkpoint(cp_key, cp)
                st["batch"]["last_id"] = last_id
            cp["batches"] = cp.get("batches", 0) + 1
            if fetched:
                save_checkpoint(cp_key, cp)
        except Exception as e:
            st["failures"].append({"ref": "-", "error": f"تعذر جلب البيانات من Maximo: {_describe_exc(e)}"})
        finally:
            st["total"] = st["done"]

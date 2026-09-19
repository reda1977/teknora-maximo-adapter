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
from datetime import datetime, timezone

import httpx

from maximo_client import MaximoClient
from teknora_client import TeknoraClient


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
    return {
        "org_id": o.get("org_id"),
        "description": o.get("description") or o.get("org_id"),
        "itemsetid": "SET1",
        "companysetid": "SET1",
        "active": True,
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
    return {
        "location_id": m.get("location"),
        "description": m.get("description") or m.get("location"),
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


def map_jobplan(m: dict) -> dict:
    return {
        "jpnum": m.get("jpnum"),
        "description": m.get("description") or m.get("jpnum"),
        "status": m.get("status") or "ACTIVE",
        "org_id": m.get("orgid"),
        "site_id": m.get("siteid"),
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
    "persons": {"os": "mxperson", "ref": "personid", "map": map_person, "save": "save_person"},
    "crafts": {"os": "mxcraft", "ref": "craft", "map": map_craft, "save": "save_craft"},
    "labor": {"os": "mxlabor", "ref": "laborcode", "map": map_labor, "save": "save_labor"},
    "locations": {"os": "mxoperloc", "ref": "location", "map": map_location, "save": "save_location"},
    "assets": {"os": "mxasset", "ref": "assetnum", "map": map_asset, "save": "save_asset"},
    "meters": {"os": "oslcmeter", "ref": "metername", "map": map_meter, "save": "save_meter"},
    "workorders": {"os": "mxwo", "ref": "wonum", "map": map_workorder, "save": "save_workorder"},
    "meterreadings": {"os": "mxmeterdata", "ref": "assetnum", "map": map_meter_reading, "save": "save_meter_reading"},
    "locationmeterreadings": {"os": "oslclocationmeter", "ref": "location", "map": map_location_meter_reading, "save": "save_location_meter_reading"},
    "jobplans": {"os": "mxapijobplan", "ref": "jpnum", "map": map_jobplan, "save": "save_jobplan"},
    "pm": {"os": "mxapipm", "ref": "pmnum", "map": map_pm, "save": "save_pm"},
}


class MigrationRun:
    def __init__(self, maximo: MaximoClient, teknora: TeknoraClient, selected_types: list):
        self.maximo = maximo
        self.teknora = teknora
        self.selected_types = set(selected_types)
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
                    await self._migrate_type(type_key, client)
        except Exception as e:
            self.state["fatal_error"] = _describe_exc(e)
        finally:
            self.state["current_type"] = None
            self.state["status"] = "done"
            self.state["finished_at"] = datetime.now(timezone.utc).isoformat()

    async def _migrate_type(self, type_key: str, client: httpx.AsyncClient):
        spec = TYPE_SPECS[type_key]
        try:
            if spec["os"] is None:
                records = await self.maximo.get_organizations_with_sites()
            else:
                records = await self.maximo.query_all(spec["os"])
        except Exception as e:
            self._init_type(type_key, 0)
            self.state["types"][type_key]["failures"].append({
                "ref": "-", "error": f"تعذر جلب البيانات من Maximo: {_describe_exc(e)}"
            })
            return

        self._init_type(type_key, len(records))
        save_fn = getattr(self.teknora, spec["save"])

        for r in records:
            ref = r.get(spec["ref"])
            try:
                await save_fn(client, spec["map"](r))
                self._record_result(type_key, ref)
            except Exception as e:
                self._record_result(type_key, ref, _describe_exc(e))

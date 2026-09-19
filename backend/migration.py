"""
منطق النقل نفسه: ترتيب الأنواع (المنظمات/المواقع الأول عشان باقي الأنواع
بترجع ليهم بمفتاح خارجي)، تحويل حقول Maximo لحقول تكنورا، وتتبع تقدم كل
نوع (نجح/فشل/الإجمالي) عشان الويزرد يعرض بروجريس بار حي ويطلع تقرير
نهائي مفصّل.

ترتيب الهجرة إجباري بسبب الروابط بين الجداول في تكنورا:
Organizations+Sites -> Locations -> Assets -> Job Plans -> Work Orders
"""
from datetime import datetime, timezone

import httpx

from maximo_client import MaximoClient
from teknora_client import TeknoraClient


def _describe_exc(e: Exception) -> str:
    """بعض الاستثناءات (زي انتهاء مهلة الاتصال) مالهاش نص وصفي في str(e) -
    فبنرجع اسم نوع الخطأ نفسه بدل رسالة فاضية غير مفيدة."""
    return str(e) or type(e).__name__


MIGRATION_ORDER = ["organizations", "locations", "assets", "jobplans", "workorders"]
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


def map_workorder(m: dict) -> dict:
    payload = {
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
    if m.get("jpnum"):
        payload["jpnum"] = m.get("jpnum")
    return payload


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
        try:
            if type_key == "organizations":
                records = await self.maximo.get_organizations_with_sites()
            elif type_key == "locations":
                records = await self.maximo.query_all("mxoperloc")
            elif type_key == "assets":
                records = await self.maximo.query_all("mxasset")
            elif type_key == "jobplans":
                records = await self.maximo.query_all("mxjobplan")
            elif type_key == "workorders":
                records = await self.maximo.query_all("mxwo")
            else:
                records = []
        except Exception as e:
            self._init_type(type_key, 0)
            self.state["types"][type_key]["failures"].append({
                "ref": "-", "error": f"تعذر جلب البيانات من Maximo: {_describe_exc(e)}"
            })
            return

        self._init_type(type_key, len(records))

        for r in records:
            ref = None
            try:
                if type_key == "organizations":
                    ref = r.get("org_id")
                    await self.teknora.save_organization(client, map_organization(r))
                elif type_key == "locations":
                    ref = r.get("location")
                    await self.teknora.save_location(client, map_location(r))
                elif type_key == "assets":
                    ref = r.get("assetnum")
                    await self.teknora.save_asset(client, map_asset(r))
                elif type_key == "jobplans":
                    ref = r.get("jpnum")
                    await self.teknora.save_jobplan(client, map_jobplan(r))
                elif type_key == "workorders":
                    ref = r.get("wonum")
                    await self.teknora.save_workorder(client, map_workorder(r))
                self._record_result(type_key, ref)
            except Exception as e:
                self._record_result(type_key, ref, _describe_exc(e))

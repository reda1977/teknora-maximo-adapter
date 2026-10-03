"""
عميل REST بتاع نظام تكنورا المركزي لإدارة الأصول (نفس الـ API اللي
بتستخدمه بوابة Teknora Portal نفسها). بيستخدم تسجيل دخول OAuth2
password-grant عادي (POST /token بصيغة form-urlencoded)، ونفس نقاط
الحفظ (save) اللي شفناها في توثيق الـ API الحقيقي (openapi.json):
/organizations/save, /locations/save, /assets/save, /workorder/save,
/jobplans/save.

كل نقاط الحفظ دي additionalProperties:true (مفيش شكل ثابت موثّق)، فبنبني
الـ payload بناءً على أسماء أعمدة SQLAlchemy الحقيقية بتاعة تكنورا
(نفس الأسماء اللي بترجع في أي GET، لأن الموديلات بتستخدم to_dict() اللي
بيرجع أسماء الأعمدة زي ما هي). أي حقل غلط هيظهر واضح في تقرير الفشل
بدل ما يتصلح بالتخمين الأعمى.
"""
from urllib.parse import quote

import httpx


class TeknoraAuthError(Exception):
    pass


class BulkUnavailable(Exception):
    """تكنورا مفيهوش الحفظ الجماعي (نسخة قديمة: 404/405) أو اليوزر مش
    SUPER_ADMIN (403) - الأداة بترجع للحفظ أمر أمر."""


class TeknoraClient:
    def __init__(self, base_url: str, username: str, password: str):
        # base_url المتوقع شامل /api (زي https://app.teknora-eam.com/api)
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.token = None

    async def login(self) -> None:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.post(
                f"{self.base_url}/token",
                data={"username": self.username, "password": self.password},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            if res.status_code != 200:
                raise TeknoraAuthError(f"فشل تسجيل الدخول لتكنورا (كود {res.status_code}): {res.text[:300]}")
            data = res.json()
            self.token = data.get("access_token")
            if not self.token:
                raise TeknoraAuthError(f"تم الاتصال لكن لم يرجع توكن دخول: {res.text[:300]}")

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json; charset=UTF-8"}

    async def _post(self, client: httpx.AsyncClient, path: str, payload: dict) -> dict:
        res = await client.post(f"{self.base_url}{path}", json=payload, headers=self._headers())
        if res.status_code == 401:
            # التوكن بيتاخد مرة واحدة بس في أول الاتصال، ولو النقل كبير (زي
            # 17811 أصل قبل PM فعليًا) بياخد وقت طويل يكفي إن التوكن ينتهي
            # منتصف الطريق - كل حاجة بعده كانت بترفض بـ 401 "Could not
            # validate credentials" من غير أي تفرقة عن خطأ بيانات حقيقي.
            # نعيد تسجيل الدخول ونجرب مرة واحدة بس قبل ما نستسلم
            await self.login()
            res = await client.post(f"{self.base_url}{path}", json=payload, headers=self._headers())
        if res.status_code >= 400:
            # ضيف اللي بعتناه فعليًا لتكنورا في رسالة الخطأ - عشان نقدر نقارن
            # مباشرة بين اسم الحقل اللي استخدمناه ورسالة الرفض (زي "Organization
            # ID is required" مع إننا بعتنا org_id) من غير تخمين اسم تاني أعمى
            raise Exception(f"HTTP {res.status_code}: {res.text[:300]} | sent={str(payload)[:200]}")
        return res.json() if res.content else {}

    async def _get(self, client: httpx.AsyncClient, path: str) -> dict:
        res = await client.get(f"{self.base_url}{path}", headers=self._headers())
        if res.status_code == 401:
            await self.login()
            res = await client.get(f"{self.base_url}{path}", headers=self._headers())
        if res.status_code >= 400:
            raise Exception(f"HTTP {res.status_code}: {res.text[:300]}")
        return res.json() if res.content else {}

    async def get_pm(self, client: httpx.AsyncClient, pmnum: str) -> dict:
        return await self._get(client, f"/pm/{quote(pmnum, safe='')}")

    async def save_organization(self, client: httpx.AsyncClient, org: dict) -> dict:
        return await self._post(client, "/organizations/save", org)

    async def save_location(self, client: httpx.AsyncClient, loc: dict) -> dict:
        return await self._post(client, "/locations/save", loc)

    async def save_asset(self, client: httpx.AsyncClient, asset: dict) -> dict:
        return await self._post(client, "/assets/save", asset)

    async def save_jobplan(self, client: httpx.AsyncClient, jp: dict) -> dict:
        return await self._post(client, "/jobplans/save", jp)

    async def save_labor(self, client: httpx.AsyncClient, labor: dict) -> dict:
        return await self._post(client, "/labor/save", labor)

    async def save_pm(self, client: httpx.AsyncClient, pm: dict) -> dict:
        return await self._post(client, "/pm/save", pm)

    async def save_workorder(self, client: httpx.AsyncClient, wo: dict) -> dict:
        return await self._post(client, "/workorder/save", wo)

    async def save_workorders_bulk(self, client: httpx.AsyncClient, wos: list) -> dict:
        """صفحة كاملة في طلب واحد - بيرجع {"results": [{"wonum", "error"}]}
        بنفس ترتيب اللي اتبعت. مهلة طويلة لأن السيرفر بيكتب 500 أمر بفرعياتهم."""
        url = f"{self.base_url}/migration/workorders/bulk"
        res = await client.post(url, json={"workorders": wos}, headers=self._headers(), timeout=600.0)
        if res.status_code == 401:
            await self.login()
            res = await client.post(url, json={"workorders": wos}, headers=self._headers(), timeout=600.0)
        if res.status_code in (403, 404, 405):
            raise BulkUnavailable(f"HTTP {res.status_code}: {res.text[:200]}")
        if res.status_code >= 400:
            raise Exception(f"HTTP {res.status_code}: {res.text[:300]}")
        return res.json()

    async def save_custom_values_bulk(self, client: httpx.AsyncClient, core_model_name: str, records: list) -> dict:
        """قيم حقول كستم لسجلات كتير (بأسماء الحقول) - PUT /custom-field-values/bulk،
        منفصل عن حفظ السجل نفسه فبيشتغل على أوامر الشغل المقفولة كمان"""
        url = f"{self.base_url}/custom-field-values/bulk"
        done = 0
        for n in range(0, len(records), 500):
            body = {"core_model_name": core_model_name, "records": records[n:n + 500]}
            res = await client.put(url, json=body, headers=self._headers(), timeout=600.0)
            if res.status_code == 401:
                await self.login()
                res = await client.put(url, json=body, headers=self._headers(), timeout=600.0)
            if res.status_code in (404, 405):
                raise BulkUnavailable(f"HTTP {res.status_code}: {res.text[:200]}")
            if res.status_code >= 400:
                raise Exception(f"HTTP {res.status_code}: {res.text[:300]}")
            done += len(body["records"])
        return {"records": done}

    async def patch_workorder_fields(self, client: httpx.AsyncClient, items: list) -> dict:
        """حقول مرجعية (pmnum) على أوامر موجودة حتى المقفولة - /migration/workorders/patch-fields"""
        url = f"{self.base_url}/migration/workorders/patch-fields"
        updated = 0
        for n in range(0, len(items), 1000):
            body = {"workorders": items[n:n + 1000]}
            res = await client.post(url, json=body, headers=self._headers(), timeout=600.0)
            if res.status_code == 401:
                await self.login()
                res = await client.post(url, json=body, headers=self._headers(), timeout=600.0)
            if res.status_code >= 400:
                raise Exception(f"HTTP {res.status_code}: {res.text[:300]}")
            updated += (res.json() or {}).get("updated") or 0
        return {"updated": updated}

    async def save_wo_custom_fields_bulk(self, client: httpx.AsyncClient, payloads: list) -> dict:
        """حقول أوامر الشغل الإضافية: رقم الـ PM على أمر الشغل نفسه، والباقي حقول كستم"""
        pm_items = [{"wonum": p["core_record_id"], "pmnum": p["pmnum"]}
                    for p in payloads if p.get("core_record_id") and p.get("pmnum")]
        if pm_items:
            await self.patch_workorder_fields(client, pm_items)
        await self.save_custom_values_bulk(
            client, "WORKORDER", [{"core_record_id": p["core_record_id"], "values": p["values"]} for p in payloads])
        return {"results": [{"error": None} for _ in payloads]}

    async def save_wo_custom_fields(self, client: httpx.AsyncClient, payload: dict) -> dict:
        return await self.save_wo_custom_fields_bulk(client, [payload])

    async def save_person(self, client: httpx.AsyncClient, person: dict) -> dict:
        return await self._post(client, "/person/save", person)

    async def save_craft(self, client: httpx.AsyncClient, craft: dict) -> dict:
        return await self._post(client, "/crafts/save", craft)

    async def save_meter(self, client: httpx.AsyncClient, meter: dict) -> dict:
        return await self._post(client, "/meters/save", meter)

    async def save_meter_reading(self, client: httpx.AsyncClient, reading: dict) -> dict:
        return await self._post(client, "/meter-readings/save", reading)

    async def save_location_meter_reading(self, client: httpx.AsyncClient, reading: dict) -> dict:
        return await self._post(client, "/locmeter-readings/save", reading)

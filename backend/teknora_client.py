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
import httpx


class TeknoraAuthError(Exception):
    pass


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
        if res.status_code >= 400:
            raise Exception(f"HTTP {res.status_code}: {res.text[:300]}")
        return res.json() if res.content else {}

    async def save_organization(self, client: httpx.AsyncClient, org: dict) -> dict:
        return await self._post(client, "/organizations/save", org)

    async def save_location(self, client: httpx.AsyncClient, loc: dict) -> dict:
        return await self._post(client, "/locations/save", loc)

    async def save_asset(self, client: httpx.AsyncClient, asset: dict) -> dict:
        return await self._post(client, "/assets/save", asset)

    async def save_jobplan(self, client: httpx.AsyncClient, jp: dict) -> dict:
        return await self._post(client, "/jobplans/save", jp)

    async def save_workorder(self, client: httpx.AsyncClient, wo: dict) -> dict:
        return await self._post(client, "/workorder/save", wo)

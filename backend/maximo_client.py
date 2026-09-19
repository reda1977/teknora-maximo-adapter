"""
عميل Maximo OSLC - نسخة مستقلة مبنية على نفس الدروس اللي اتعلمناها فعليًا
من الربط الحقيقي في Teknora Portal (backend/app/routers/portal.py):

- المصادقة بـ header اسمه "maxauth" (base64 لـ username:password)، مش
  Authorization: Basic القياسي - النسخة دي من Maximo بترفضه بالتحديد.
- استعلام المجموعة (collection) بيرجع روابط بس (rdfs:member/rdf:resource)،
  مفيش بيانات فعلية inline حتى مع lean=1 أو oslc.properties=* - لازم
  نتبع كل رابط ونجيب السجل بعينه.
- كل الحقول في رد السجل بعينه بتيجي ببادئة namespace "spi:" (زي
  spi:assetnum)، ولازم نفس البادئة في oslc.where وفي أي payload بنبعته
  (وإلا القيمة بتترجم NULL على السيرفر من غير أي خطأ واضح).
- الإنشاء (POST) أحيانًا بيرجع 201 من غير أي محتوى - المعرّف بيجي في
  ترويسة Location بس، فمحتاجين نتبعها لو الـ body فاضي.
"""
import base64
import httpx


class MaximoAuthError(Exception):
    pass


def _strip_spi_prefix(d: dict) -> dict:
    return {k.split(":", 1)[-1]: v for k, v in d.items() if isinstance(k, str)}


class MaximoClient:
    def __init__(self, base_url: str, username: str, password: str):
        # base_url المتوقع من غير / في الآخر، وشامل /maximo (زي
        # http://172.18.0.1:9080/maximo)
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self._maxauth = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")

    def _headers(self) -> dict:
        return {"maxauth": self._maxauth}

    async def test_connection(self) -> None:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.get(f"{self.base_url}/oslc/login", headers=self._headers())
            if res.status_code != 200:
                raise MaximoAuthError(f"فشل تسجيل الدخول لـ Maximo (كود {res.status_code}): {res.text[:300]}")

    async def query_all(self, object_structure: str, where: str = None, page_size: int = 200,
                         concurrency: int = 5) -> list:
        """بيرجع كل سجلات Object Structure معين كاملة (مش مجرد روابط)،
        مع دعم صفحات لو المجموعة كبيرة."""
        member_refs = []
        async with httpx.AsyncClient(timeout=30.0) as client:
            url = f"{self.base_url}/oslc/os/{object_structure}"
            params = {"oslc.pageSize": str(page_size)}
            if where:
                params["oslc.where"] = where

            next_url = url
            next_params = params
            seen_urls = set()
            while next_url and next_url not in seen_urls:
                seen_urls.add(next_url)
                res = await client.get(next_url, params=next_params, headers=self._headers())
                res.raise_for_status()
                data = res.json()
                refs = data.get("member") or data.get("rdfs:member") or []
                member_refs.extend(refs)

                # بحث عن رابط الصفحة الجاية لو موجود (اسمه بيختلف حسب النسخة)
                response_info = data.get("oslc:responseInfo") or data.get("responseInfo") or {}
                next_page = response_info.get("oslc:nextPage") or response_info.get("nextPage")
                if next_page:
                    next_url = next_page
                    next_params = None
                else:
                    next_url = None

            # نتبع كل رابط سجل عشان نجيب بياناته الكاملة (بحد أقصى للتزامن
            # عشان منضربش السيرفر بيها كلها مرة واحدة)
            import asyncio
            sem = asyncio.Semaphore(concurrency)
            results = [None] * len(member_refs)

            async def fetch_one(i, ref):
                if not isinstance(ref, dict):
                    return
                if len(ref) > 1 or "rdf:resource" not in ref:
                    results[i] = _strip_spi_prefix(ref)
                    return
                resource_url = ref.get("rdf:resource")
                if not resource_url:
                    return
                record_id = resource_url.rstrip("/").split("/")[-1]
                async with sem:
                    detail_res = await client.get(
                        f"{self.base_url}/oslc/os/{object_structure}/{record_id}",
                        headers=self._headers(),
                    )
                    detail_res.raise_for_status()
                    results[i] = _strip_spi_prefix(detail_res.json())

            await asyncio.gather(*[fetch_one(i, ref) for i, ref in enumerate(member_refs)])
            return [r for r in results if r]

    async def count_collection(self, object_structure: str, where: str = None) -> int:
        """بيرجع عدد السجلات بسرعة (استعلام واحد بس، من غير ما نتبع كل
        رابط سجل) - مستخدمة في شاشة المعاينة قبل بدء النقل الفعلي."""
        async with httpx.AsyncClient(timeout=20.0) as client:
            params = {"oslc.pageSize": "1000"}
            if where:
                params["oslc.where"] = where
            res = await client.get(
                f"{self.base_url}/oslc/os/{object_structure}",
                params=params,
                headers=self._headers(),
            )
            res.raise_for_status()
            data = res.json()
            members = data.get("member") or data.get("rdfs:member") or []
            return len(members)

    async def get_organizations_with_sites(self) -> list:
        """بيرجع كل المنظمات، كل واحدة ومواقعها المتداخلة (site relationship
        جوه MXORGANIZATION - مفيش object structure منفصل للمواقع)."""
        orgs_raw = await self.query_all("mxorganization")
        orgs = []
        for o in orgs_raw:
            sites_raw = o.get("site") or []
            orgs.append({
                "org_id": o.get("orgid"),
                "description": o.get("description"),
                "sites": [
                    {"site_id": s.get("spi:siteid"), "description": s.get("spi:description")}
                    for s in sites_raw
                ],
            })
        return orgs

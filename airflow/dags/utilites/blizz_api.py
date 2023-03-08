import requests
import asyncio
import aiohttp
from requests.adapters import HTTPAdapter, Retry

API_TOKEN_URL = "https://eu.battle.net/oauth/token"
BLIZZ_URL = "https://%s.api.blizzard.com"


AUCTION_PATH = "/data/wow/connected-realm/%d/auctions?"
ITEMS_PATH = "/data/wow/item/%d?"
TOKEN_PATH = "/data/wow/token/index?"

LOCALE = "ru_RU"
NAMESPACE_DYNAMIC = "dynamic-%s"
NAMESPACE_STATIC = "static-%s"


class BlizzardApi:
    def __init__(self, client_id: str, client_secret: str):
        self._client_id = client_id
        self._client_secret = client_secret
        self._token = None

    async def one_request(self, session, region: str, item_id: int, limit):
        params = {
            "namespace": NAMESPACE_STATIC % region,
            "locale": LOCALE,
            "access_token": self._token
        }
        async with limit:
            response = await session.get(
                f"{BLIZZ_URL % region}{ITEMS_PATH % item_id}",
                params=params, ssl=False
            )
            if limit.locked():
                await asyncio.sleep(1)
        return response

    async def get_data_items(self, region: str, items_id: int, limit_num: int):
        limit = asyncio.Semaphore(limit_num)
        tasks = []
        async with aiohttp.ClientSession() as session:
            for item_id in items_id:
                tasks.append(
                    asyncio.create_task(
                        self.one_request(session, region, item_id, limit)
                    )
                )
            responses = await asyncio.gather(*tasks)
        results = [
            await response.json(content_type=None)
            for response in responses
        ]
        return results

    def get_data_auction(self, region: str, realm_id: int):
        params = {
            "namespace": NAMESPACE_DYNAMIC % region,
            "locale": LOCALE,
            "access_token": self._token
        }
        try:
            session = requests.Session()
            response = session.get(
                f"{BLIZZ_URL % region}{AUCTION_PATH % realm_id}",
                params=params
            )
            response = response.json()
            return response["auctions"]
        except requests.exceptions.RequestException as error:
            raise error

    def get_data_token(self, region: str) -> dict:
        params = {
            "namespace": NAMESPACE_DYNAMIC % region,
            "locale": LOCALE,
            "access_token": self._token
        }
        try:
            session = requests.Session()
            retries = Retry(total=5, backoff_factor=0.2)
            session.mount('https://', HTTPAdapter(max_retries=retries))
            response = session.get(
                f"{BLIZZ_URL % region}{TOKEN_PATH}", params=params
            )
            response = response.json()
            data = {
                "region": region,
                "last_updated_timestamp": response["last_updated_timestamp"],
                "price": response["price"]
            }
            return data
        except requests.exceptions.RequestException as error:
            raise error

    def get_access_token(self) -> str:
        data = {"grant_type": "client_credentials"}
        auth = (self._client_id, self._client_secret)
        try:
            response = requests.post(API_TOKEN_URL, data=data, auth=auth)
            if response.status_code == 200:
                token = response.json()
                self._token = token["access_token"]
        except requests.exceptions.RequestException as error:
            raise error

import json
import logging
import os
from typing import Any

from pybeansack.cdnstore import AsyncCDNStore
from slugify import slugify
from datetime import datetime
from .utils import *
from pybeansack import Beansack, create_client
from pybeansack.models import (
    K_BASE_URL,
    K_CATEGORIES,
    K_SENTIMENTS,
    K_URL,
    K_KIND,
    K_SOURCE,
    K_TITLE,
    K_TITLE_LENGTH,
    K_SUMMARY,
    K_SUMMARY_LENGTH,
    K_CONTENT,
    K_CONTENT_LENGTH,
    K_RESTRICTED_CONTENT,
    K_IMAGEURL,
    K_AUTHOR,
    K_CREATED,
    K_COLLECTED,
    K_EMBEDDING,
    K_ENTITIES,
    K_REGIONS,
    Bean,
)
from pybeansack.utils import *
from icecream import ic

log = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", os.cpu_count()))
PORT_CONDITIONS = []
PORT_COLUMNS = [
    K_URL,
    K_KIND,
    K_SOURCE,
    K_TITLE,
    K_TITLE_LENGTH,
    K_SUMMARY,
    K_SUMMARY_LENGTH,
    K_CONTENT,
    K_CONTENT_LENGTH,
    K_RESTRICTED_CONTENT,
    K_IMAGEURL,
    K_AUTHOR,
    K_CREATED,
    K_COLLECTED,
    K_EMBEDDING,
    K_ENTITIES,
    K_REGIONS,
]

_CDN_PATH_TEMPLATE = "beansack/contents/{date}/{slugurl}"
cdn_path = lambda bean: _CDN_PATH_TEMPLATE.format(
    date=bean[K_CREATED].strftime("%Y/%m/%d"), slugurl=slugify(bean[K_URL])
)


class Orchestrator:
    db: Beansack
    cdn: AsyncCDNStore
    run_total: int = 0

    def __init__(self, state_store, db: Beansack, cdn_kwargs: dict):
        self.state_store = state_store
        self.db = db

    async def run_cdn_porter(self):
        async with self.states:
            beans = await self.states.get(
                "beans",
                states="collected",
                exclude_states="cdned",
                conditions=[K_CONTENT],
                columns=[K_URL, K_CREATED, K_SOURCE, K_CONTENT],
            )
            log.info(
                "starting cdn",
                extra={
                    "num_items": len(beans),
                    "source": datetime.now().strftime("%Y-%m-%d"),
                },
            )
            [bean.update({"path": cdn_path(bean)}) for bean in beans]
            cdn_urls = await self.cdn.batch_upload_texts(beans)
            updates = list(
                map(
                    lambda bean, cdn_url: {K_URL: bean[K_URL], "content": cdn_url},
                    beans,
                    cdn_urls,
                )
            )
            await self.states.set("beans", "cdned", ic(updates))
            log.info(
                "total cdned",
                extra={
                    "num_items": len(beans),
                    "source": datetime.now().strftime("%Y-%m-%d"),
                },
            )

    def _store(self, beans: list[dict]):
        if not beans:
            return
        ic(len(beans))
        with open(f".test/{int(datetime.now().timestamp())}.json", "w") as f:
            json.dump(
                [
                    Bean(**bean).model_dump(
                        mode="json", exclude_none=True, exclude_unset=True
                    )
                    for bean in beans
                ],
                f,
                indent=4,
            )

    def run(self):
        beans = self.state_store.get(
            "beans",
            states=["collected", "embedded", "classified"],
            exclude_states="ported"
        )
        beans = merge(K_URL, beans)
        self._store(beans)

    def close(self):
        self.db.close()


def merge(key, items: list[dict[str, Any]]):
    merged = {}
    for data in items:
        update = {k: v for k, v in data.items() if v}
        if data[key] not in merged: merged[data[key]] = update
        else: merged[data[key]].update(update)
        # NOTE: it is important to update only the keys for which there is a value
            
    return list(merged.values())

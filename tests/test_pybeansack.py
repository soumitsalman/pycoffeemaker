"""Integration tests for pybeansack backends."""

import logging
import random
import sys
from pathlib import Path

import numpy as np
import pytest
from faker import Faker
from icecream import ic

from pybeansack.database import BEANS, CHATTERS, PUBLISHERS
from pybeansack.models import *
from utils.config import VECTOR_LEN
from utils.dates import ndays_ago

logging.basicConfig(level=logging.INFO)
faker = Faker()

TREND_COLUMNS = [URL, TITLE, CATEGORIES, LIKES, COMMENTS, RELATED, TRENDSCORE]

ALL_BACKENDS = [
    pytest.param("pg_db", marks=pytest.mark.pg),
    pytest.param("duck_db", marks=pytest.mark.duck),
    pytest.param("lance_db", marks=pytest.mark.lance),
]
SQL_BACKENDS = [
    pytest.param("pg_db", marks=pytest.mark.pg),
    pytest.param("duck_db", marks=pytest.mark.duck),
]


def random_embedding() -> list[float]:
    return np.random.random(VECTOR_LEN).tolist()


def generate_fake_beans(ai_fields: bool = True, limit: int = 30) -> list[Bean]:
    def _one() -> Bean:
        return Bean(
            url=faker.url(),
            kind=faker.random_element(elements=("news", "blog")),
            source=faker.domain_name(),
            title=faker.sentence(nb_words=6),
            summary=faker.paragraph(nb_sentences=5),
            content=faker.paragraph(nb_sentences=10),
            image_url=faker.image_url(),
            author=faker.name(),
            created=faker.date_time_this_year(),
            collected=faker.date_time_this_month(),
            embedding=random_embedding() if ai_fields else None,
            categories=[faker.word() for _ in range(3)] if ai_fields else None,
            sentiments=[faker.word() for _ in range(3)] if ai_fields else None,
            entities=[faker.name() for _ in range(3)] if ai_fields else None,
            regions=[faker.country(), faker.city()] if ai_fields else None,
        )

    return [_one() for _ in range(random.randrange(10, limit))]


def generate_fake_publishers(sources: list[str] = None, update: bool = False) -> list[Publisher]:
    prefix = "[UPDATED]" if update else ""

    def _one(source: str = None) -> Publisher:
        return Publisher(
            source=source or faker.domain_name(),
            base_url=faker.url(),
            site_name=(prefix + faker.company()) if random.random() > 0.5 else None,
            description=(prefix + faker.sentence(nb_words=10)) if random.random() > 0.5 else None,
            favicon=(prefix + faker.image_url()) if random.random() > 0.5 else None,
            rss_feed=(prefix + faker.url()) if random.random() > 0.5 else None,
        )

    if sources:
        return [_one(source) for source in sources]
    return [_one() for _ in range(random.randrange(5, 15))]


def generate_fake_chatters() -> list[Chatter]:
    def _one() -> Chatter:
        return Chatter(
            chatter_url=faker.url(),
            url=faker.url(),
            source=faker.domain_name(),
            forum=faker.random_element(elements=("r/technology", "r/programming", "HackerNews", "Lobsters")),
            collected=faker.date_time_this_month(),
            likes=faker.random_int(min=0, max=1000),
            comments=faker.random_int(min=0, max=500),
            subscribers=faker.random_int(min=0, max=10000),
        )

    return [_one() for _ in range(random.randrange(10, 30))]


def generate_fake_related(urls: list[str]) -> list[dict]:
    return [{"url": url, "related_url": faker.url()} for url in urls]


def generate_fake_extractions(urls: list[str]) -> list[Bean]:
    return [
        Bean(
            url=url,
            entities=["[UPDATED]" + faker.name() for _ in range(3)],
            regions=["[UPDATED]" + faker.country(), "[UPDATED]" + faker.city()],
        )
        for url in urls
    ]


def _store_and_query(db):
    beans = generate_fake_beans(ai_fields=True, limit=15)
    publishers = generate_fake_publishers(sources=list({b.source for b in beans if b.source})[:5])
    chatters = generate_fake_chatters()
    related = generate_fake_related([b.url for b in beans[:3]])

    ic(db.count_rows(BEANS))
    ic(db.store_beans(beans))
    ic(db.store_publishers(publishers))
    ic(db.store_chatters(chatters))
    ic(db.store_related(related))
    ic(db.count_rows(BEANS), db.count_rows(PUBLISHERS), db.count_rows(CHATTERS))

    ic(db.query_latest_beans(limit=5, columns=[URL, TITLE, KIND]))
    ic(db.query_publishers(limit=5, columns=[SOURCE, SITE_NAME]))


def _updates(db):
    beans = db.query_latest_beans(limit=2, columns=[URL, ENTITIES, REGIONS])
    urls = [bean.url for bean in beans]
    ic(db.update_beans(generate_fake_extractions(urls), columns=[ENTITIES, REGIONS]))

    publishers = db.query_publishers(limit=5)
    sources = [pub.source for pub in publishers if pub.source]
    if not sources:
        return
    ic(db.update_publishers(generate_fake_publishers(sources, update=True)))
    ic(db.query_publishers(sources=sources, columns=[SOURCE, SITE_NAME, DESCRIPTION]))


def _trend_queries(db):
    ic(db.optimize())
    ic(db.query_trending_beans(limit=5, columns=[URL, TRENDSCORE, LIKES, RELATED]))
    ic(
        db.query_aggregated_beans(
            created=ndays_ago(360),
            categories=["Algorithm and Computation", "Human Biology and Physiology"],
            columns=TREND_COLUMNS,
            limit=5,
        )
    )
    ic(
        db.query_aggregated_beans(
            kind="news",
            embedding=random_embedding(),
            distance=10,
            columns=[URL, KIND, TITLE, *TREND_COLUMNS[2:]],
            limit=5,
        )
    )


def _deduplicate(db):
    beans = generate_fake_beans(limit=5)
    beans[0].url = "https://example.com/article-1"
    beans[1].url = "https://example.com/article-2"
    ic(len(beans), len(db.deduplicate(BEANS, beans)))


@pytest.mark.integration
@pytest.mark.parametrize("db", ALL_BACKENDS, indirect=True)
def test_store_and_query(db):
    _store_and_query(db)


@pytest.mark.integration
@pytest.mark.parametrize("db", SQL_BACKENDS, indirect=True)
def test_trend_queries(db):
    _trend_queries(db)


@pytest.mark.integration
@pytest.mark.pg
def test_updates(pg_db):
    _updates(pg_db)


@pytest.mark.integration
@pytest.mark.pg
def test_deduplicate(pg_db):
    _deduplicate(pg_db)


if __name__ == "__main__":
    raise SystemExit(pytest.main([str(Path(__file__).resolve()), *sys.argv[1:]]))

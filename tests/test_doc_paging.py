"""Raw-document reads, offline with a fake transport: page by page, no cap with a LIMIT; one search when the
documents fit a page whatever the LIMIT; a scroll on a cluster that does not page a point in time on _shard_doc."""

from __future__ import annotations

from zoneinfo import ZoneInfo

import pytest

from osagg.errors import ProgrammingError, PushdownError
from osagg.executor import Executor
from osagg.metadata import Field
from osagg.planner import DocScan
from osagg.transport import Transport

FIELDS = [Field("APP", "keyword", "VARCHAR", "APP", "APP"),
          Field("N", "long", "BIGINT", "N", "N")]


class FakeTransport(Transport):
    kind = "direct"

    def __init__(self, n_docs: int, shard_doc: bool = True, cluster: str = "fake:9200") -> None:
        self.docs = [{"_id": str(i), "_source": {"APP": f"a{i % 7}", "N": i}, "sort": [i]}
                     for i in range(n_docs)]
        self.sizes: list[int] = []
        self.shard_doc, self.cluster_key = shard_doc, cluster
        self.pits_opened = self.pits_closed = self.scrolls = self.scrolls_cleared = 0

    def search(self, index, body, timeout=None):
        if "pit" in body and not self.shard_doc and any("_shard_doc" in s for s in body.get("sort") or []):
            raise ProgrammingError("OpenSearch rejected the request: query_shard_exception: No mapping found for "
                                   "[_shard_doc] in order to sort on (all shards failed)")
        start = body.get("search_after", [-1])[0] + 1
        page = self.docs[start:start + body["size"]]
        self.sizes.append(len(page))
        return {"took": 1, "hits": {"hits": page}, "pit_id": "pit"}

    def count(self, index, query):
        return len(self.docs)

    def open_pit(self, index, keep_alive="2m"):
        self.pits_opened += 1
        return "pit"

    def close_pit(self, pit_id):
        self.pits_closed += 1

    def scroll_pages(self, index, body, keep_alive="2m", timeout=None):
        self.scrolls += 1
        try:
            for start in range(0, len(self.docs) + 1, body["size"]):
                page = self.docs[start:start + body["size"]]
                self.sizes.append(len(page))
                yield {"took": 1, "hits": {"hits": page}, "_scroll_id": "s"}
                if not page:
                    return
        finally:
            self.scrolls_cleared += 1


def run(n_docs, limit, tr=None, **kw):
    tr = tr or FakeTransport(n_docs)
    ex = Executor(tr, ZoneInfo("Europe/Paris"), doc_page_size=1000, **kw)
    table, stats = ex.run(DocScan("t", "idx", {"match_all": {}}, FIELDS, None, limit))
    return table, stats, tr


def test_limit_above_the_scan_cap_is_read_page_by_page():
    table, stats, tr = run(7_500, limit=7_500, max_scan_rows=1_000)
    assert table.num_rows == 7_500 and stats.rows == 7_500
    assert tr.sizes == [1000] + [1000] * 7 + [500]        # the first search, then 8 pages of a point in time
    assert tr.pits_opened == tr.pits_closed == 1
    assert table.column("N").to_pylist() == list(range(7_500))


def test_optional_max_rows_caps_a_limit():
    table, _, _ = run(5_000, limit=5_000, max_rows=2_500)
    assert table.num_rows == 2_500


def test_without_limit_the_scan_cap_applies():
    with pytest.raises(PushdownError, match="max_scan_rows"):
        run(5_000, limit=None, max_scan_rows=1_000)
    table, _, _ = run(900, limit=None, max_scan_rows=1_000)
    assert table.num_rows == 900


def test_a_large_limit_over_few_documents_is_one_search():
    """SQL Lab's LIMIT 100000 over 300 matching documents: one search, no point in time (a cluster that refuses
    the _shard_doc sort never sees it)."""
    tr = FakeTransport(300, shard_doc=False)
    table, stats, _ = run(300, limit=100_000, tr=tr)
    assert table.num_rows == 300 and stats.requests == 1
    assert tr.pits_opened == 0 and tr.scrolls == 0
    table, _, _ = run(1000, limit=100_000, tr=FakeTransport(1000))    # exactly a page: read deeper (may be more)
    assert table.num_rows == 1000


def test_a_cluster_without_shard_doc_is_read_by_scroll_and_remembered():
    from osagg.executor import NO_SHARD_DOC

    NO_SHARD_DOC.discard("old:9200")
    tr = FakeTransport(4_200, shard_doc=False, cluster="old:9200")
    table, stats, _ = run(4_200, limit=100_000, tr=tr)
    assert table.num_rows == 4_200 and table.column("N").to_pylist() == list(range(4_200))   # each once, in order
    assert tr.pits_opened == tr.pits_closed == 1 and tr.scrolls == tr.scrolls_cleared == 1
    assert "old:9200" in NO_SHARD_DOC
    tr2 = FakeTransport(2_500, shard_doc=False, cluster="old:9200")            # the next query: scroll at once
    table, _, _ = run(2_500, limit=2_200, tr=tr2)
    assert table.num_rows == 2_200 and tr2.pits_opened == 0 and tr2.scrolls == tr2.scrolls_cleared == 1
    tr3 = FakeTransport(2_500, cluster="new:9200")                              # another cluster: a point in time
    run(2_500, limit=100_000, tr=tr3)
    assert tr3.pits_opened == 1 and tr3.scrolls == 0
    NO_SHARD_DOC.discard("old:9200")


def test_without_limit_a_counted_large_read_needs_no_first_search():
    tr = FakeTransport(2_500)
    table, stats, _ = run(2_500, limit=None, tr=tr, max_scan_rows=10_000)
    assert table.num_rows == 2_500 and tr.sizes == [1000, 1000, 500] and tr.pits_opened == 1


def test_another_error_of_a_point_in_time_is_not_hidden():
    class Broken(FakeTransport):
        def search(self, index, body, timeout=None):
            if "pit" in body:
                raise ProgrammingError("OpenSearch rejected the request: search_phase_execution_exception")
            return super().search(index, body, timeout)

    with pytest.raises(ProgrammingError, match="search_phase_execution_exception"):
        run(3_000, limit=100_000, tr=Broken(3_000, cluster="x:1"))

import types
import pandas as pd
import pytest

from dataiq.oracle_connector import OracleConnector


class DummyConn:
    def __init__(self):
        self.closed = False
    def execute(self, *args, **kwargs):
        class R:
            def __iter__(self_inner):
                return iter([(1,)])
        return R()
    def close(self):
        self.closed = True
    def __enter__(self):
        return self
    def __exit__(self, *args):
        self.close()


def test_test_connection(monkeypatch):
    oc = OracleConnector()
    monkeypatch.setattr(oc, "connect", lambda: DummyConn())
    assert oc.test_connection() is True


def test_get_table_names(monkeypatch):
    oc = OracleConnector()
    def fake_connect():
        c = DummyConn()
        def exec_tables(q, *args, **kwargs):
            class R:
                def __iter__(self_inner):
                    return iter([("T1",), ("T2",)])
            return R()
        c.execute = exec_tables
        return c
    monkeypatch.setattr(oc, "connect", fake_connect)
    tables = oc.get_table_names()
    assert tables == ["T1", "T2"]


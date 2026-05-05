"""Dagster assets (ingest, stage, model).

Individual modules (``raw``, ``staging``, ``dimensions``, ``analytics``) register ``@asset`` callables;
``definitions.py`` loads them via ``load_assets_from_modules``.
"""

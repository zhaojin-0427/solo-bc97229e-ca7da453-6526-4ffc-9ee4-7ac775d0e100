"""Mapping of storage domain errors to HTTP responses."""

from __future__ import annotations

from fastapi import Request
from fastapi.responses import JSONResponse

from .storage import StoreError


def register_exception_handlers(app) -> None:
    @app.exception_handler(StoreError)
    async def _store_error_handler(_request: Request, exc: StoreError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status,
            content={"error": exc.code, "message": exc.message, "details": exc.details},
        )

"""Pydantic request/response models for the HTTP boundary."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CreateQueueRequest(BaseModel):
    name: str
    payload_schema: dict[str, Any] = Field(default_factory=dict)


class PushRequest(BaseModel):
    payload: dict[str, Any]


class ClaimRequest(BaseModel):
    consumer_id: str


class HeartbeatRequest(BaseModel):
    task_id: str


class FinishRequest(BaseModel):
    task_id: str

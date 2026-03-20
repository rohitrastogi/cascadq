"""Pydantic request/response models for the HTTP boundary."""

from __future__ import annotations

from pydantic import BaseModel


class CreateQueueRequest(BaseModel):
    name: str
    payload_schema: dict = {}


class PushRequest(BaseModel):
    payload: dict


class ClaimRequest(BaseModel):
    consumer_id: str


class HeartbeatRequest(BaseModel):
    task_id: str


class FinishRequest(BaseModel):
    task_id: str


class TaskResponse(BaseModel):
    task_id: str


class ClaimResponse(BaseModel):
    task_id: str
    payload: dict

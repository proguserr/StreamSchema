import os
import metrics

import os, hashlib, json
import orjson
from datetime import datetime
from typing import Literal
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import Integer, String, DateTime, Text, ForeignKey, select, UniqueConstraint
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
from jsonschema import Draft202012Validator, validators  
metrics.start_metrics(port=int(os.getenv("METRICS_PORT", "9100")))
from metrics import SCHEMA_LOOKUPS, SCHEMA_REGISTRATIONS

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./dev.db")

REG_SCHEMAS = Counter("ss_registry_schemas_total", "Schemas registered", ["subject"])
REG_COMPAT = Counter("ss_registry_compat_checks_total", "Compatibility checks", ["mode","result"])
REG_DB_LAT = Histogram("ss_registry_db_latency_seconds", "DB latency")

def canonical_json(obj) -> bytes:
    return orjson.dumps(obj, option=orjson.OPT_SORT_KEYS)

def fingerprint(schema_obj: dict) -> str:
    return hashlib.sha256(canonical_json(schema_obj)).hexdigest()

class Base(DeclarativeBase): pass

class Subject(Base):
    __tablename__ = "subjects"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    compatibility_mode: Mapped[str] = mapped_column(String(16), default="BACKWARD")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

class Schema(Base):
    __tablename__ = "schemas"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    subject_id: Mapped[int] = mapped_column(ForeignKey("subjects.id", ondelete="CASCADE"))
    version: Mapped[int] = mapped_column(Integer)
    fingerprint: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    schema_json: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    __table_args__ = (UniqueConstraint("subject_id","version", name="uq_subject_version"),)

engine = create_async_engine(DATABASE_URL, echo=False, future=True)
Session = async_sessionmaker(engine, expire_on_commit=False)

CompatMode = Literal["BACKWARD","FORWARD","FULL","NONE"]

def _extend_with_default(validator_class):
    validate_properties = validator_class.VALIDATORS["properties"]
    def set_defaults(validator, properties, instance, schema):
        for prop, subschema in properties.items():
            if "default" in subschema:
                instance.setdefault(prop, subschema["default"])
        for error in validate_properties(validator, properties, instance, schema):
            yield error
    return validators.extend(validator_class, {"properties": set_defaults})

DefaultingValidator = _extend_with_default(Draft202012Validator)

def check_backward_compatible(old: dict, new: dict) -> bool:
    oprops = old.get("properties", {})
    nprops = new.get("properties", {})
    for k, v in oprops.items():
        if k not in nprops: return False
        if v.get("type") != nprops[k].get("type"): return False
    return True

def check_forward_compatible(old: dict, new: dict) -> bool:
    nreq = set(new.get("required", []))
    oreq = set(old.get("required", []))
    if not nreq.issubset(oreq | set()): return False
    return check_backward_compatible(old, new)

def check_full_compatible(old: dict, new: dict) -> bool:
    return check_backward_compatible(old, new) and check_forward_compatible(old, new)

def is_compatible(mode: str, old: dict, new: dict) -> bool:
    if mode == "NONE": return True
    if mode == "BACKWARD": return check_backward_compatible(old, new)
    if mode == "FORWARD": return check_forward_compatible(old, new)
    if mode == "FULL": return check_full_compatible(old, new)
    return False

class SubjectCreate(BaseModel):
    compatibility_mode: CompatMode = "BACKWARD"

class SchemaRegister(BaseModel):
    schema: dict

class CompatCheck(BaseModel):
    candidate: dict

app = FastAPI(title="StreamSchema Registry")

@app.on_event("startup")
async def on_startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/subjects/{name}")
async def create_subject(name: str, body: SubjectCreate):
    async with Session() as s:
        async with s.begin():
            subj = Subject(name=name, compatibility_mode=body.compatibility_mode)
            s.add(subj)
    return {"status": "ok"}

@app.put("/subjects/{name}/compatibility")
async def set_compat(name: str, mode: CompatMode):
    async with Session() as s:
        res = await s.execute(select(Subject).where(Subject.name==name))
        subj = res.scalar_one_or_none()
        if not subj: raise HTTPException(404, "subject not found")
        subj.compatibility_mode = mode
        await s.commit()
    return {"status":"ok"}

from sqlalchemy import select
@app.get("/subjects/{name}/versions/latest")
async def latest(name: str):
    SCHEMA_LOOKUPS.inc()  # increment lookup metric
    async with Session() as s:
        res = await s.execute(select(Subject).where(Subject.name==name))
        subj = res.scalar_one_or_none()
        if not subj: raise HTTPException(404, "subject not found")
        res = await s.execute(select(Schema).where(Schema.subject_id==subj.id).order_by(Schema.version.desc()))
        sch = res.scalars().first()
        if not sch: raise HTTPException(404, "no schema")
        return {"subject": name, "version": sch.version, "fingerprint": sch.fingerprint, "schema": json.loads(sch.schema_json)}

@app.get("/schemas/{fingerprint_}")
async def get_schema(fingerprint_: str):
    async with Session() as s:
        res = await s.execute(select(Schema).where(Schema.fingerprint==fingerprint_))
        sch = res.scalar_one_or_none()
        if not sch: raise HTTPException(404, "not found")
        return {"version": sch.version, "schema": json.loads(sch.schema_json)}

@app.post("/compatibility/subjects/{name}")
async def compat(name: str, body: CompatCheck):
    async with Session() as s:
        res = await s.execute(select(Subject).where(Subject.name==name))
        subj = res.scalar_one_or_none()
        if not subj: raise HTTPException(404, "subject not found")
        res = await s.execute(select(Schema).where(Schema.subject_id==subj.id).order_by(Schema.version.desc()))
        latest = res.scalars().first()
        if not latest:
            return {"compatible": True, "mode": subj.compatibility_mode, "reason": "no prior schema"}
        old = json.loads(latest.schema_json)
        ok = is_compatible(subj.compatibility_mode, old, body.candidate)
        return {"compatible": ok, "mode": subj.compatibility_mode}

@app.post("/subjects/{name}/versions")
async def register_schema(name: str, body: SchemaRegister):
    SCHEMA_REGISTRATIONS.inc()  # increment registration metric
    schema = body.schema
    try:
        DefaultingValidator(schema).check_schema(schema)
    except Exception as e:
        raise HTTPException(400, f"invalid schema: {e}")
    fp = fingerprint(schema)
    async with Session() as s:
        res = await s.execute(select(Subject).where(Subject.name==name))
        subj = res.scalar_one_or_none()
        if not subj: raise HTTPException(404, "subject not found")
        res = await s.execute(select(Schema).where(Schema.subject_id==subj.id).order_by(Schema.version.desc()))
        latest = res.scalars().first()
        if latest:
            old = json.loads(latest.schema_json)
            if not is_compatible(subj.compatibility_mode, old, schema):
                raise HTTPException(409, "schema incompatible with mode")
        next_ver = (latest.version + 1) if latest else 1
        sch = Schema(subject_id=subj.id, version=next_ver, fingerprint=fp, schema_json=json.dumps(schema))
        s.add(sch); await s.commit()
        return {"subject": name, "version": next_ver, "fingerprint": fp}

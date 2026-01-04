from sqlalchemy import BigInteger, Column, Date, DateTime, Float, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB

from Backend.database import Base

# NOTE:
# These ORM models exist primarily so Alembic autogenerate can "see" the schema via Base.metadata.
# The health ingestion/rollup pipeline uses explicit SQL for performance and clarity.


class MainHealthMetric(Base):
    __tablename__ = "main_health_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Text, index=True, nullable=False)
    timestamp = Column(DateTime(timezone=True), index=True, nullable=False)
    end_ts = Column(DateTime(timezone=True), nullable=True)

    metric_type = Column(Text, index=True, nullable=False)
    metric_value = Column(Float, nullable=False)
    unit = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    meta = Column(JSONB, nullable=True)
    hk_uuid = Column(Text, nullable=True)
    deleted_at = Column(DateTime(timezone=True), nullable=True)
    hk_source_bundle_id = Column(Text, nullable=True)
    hk_source_name = Column(Text, nullable=True)
    hk_source_version = Column(Text, nullable=True)
    hk_metadata = Column(JSONB, nullable=True)


class MainHealthEvent(Base):
    __tablename__ = "main_health_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Text, index=True, nullable=False)
    timestamp = Column(DateTime(timezone=True), index=True, nullable=False)
    end_ts = Column(DateTime(timezone=True), nullable=True)

    event_type = Column(Text, index=True, nullable=False)
    value = Column(Float, nullable=False)
    unit = Column(Text, nullable=True)
    source = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    hk_uuid = Column(Text, nullable=True)
    deleted_at = Column(DateTime(timezone=True), nullable=True)
    hk_source_bundle_id = Column(Text, nullable=True)
    hk_source_name = Column(Text, nullable=True)
    hk_source_version = Column(Text, nullable=True)
    hk_metadata = Column(JSONB, nullable=True)


class DerivedRollupHourly(Base):
    __tablename__ = "derived_rollup_hourly"

    user_id = Column(Text, primary_key=True)
    metric_type = Column(Text, primary_key=True)
    bucket_ts = Column(DateTime(timezone=True), primary_key=True)

    avg_value = Column(Float, nullable=True)
    sum_value = Column(Float, nullable=True)
    min_value = Column(Float, nullable=True)
    max_value = Column(Float, nullable=True)
    n = Column(BigInteger, nullable=True)

    hk_sources = Column(JSONB, nullable=True)
    meta = Column(JSONB, nullable=True)


class DerivedRollupDaily(Base):
    __tablename__ = "derived_rollup_daily"

    user_id = Column(Text, primary_key=True)
    metric_type = Column(Text, primary_key=True)
    bucket_ts = Column(DateTime(timezone=True), primary_key=True)

    avg_value = Column(Float, nullable=True)
    sum_value = Column(Float, nullable=True)
    min_value = Column(Float, nullable=True)
    max_value = Column(Float, nullable=True)
    n = Column(BigInteger, nullable=True)

    hk_sources = Column(JSONB, nullable=True)
    meta = Column(JSONB, nullable=True)


class DerivedWorkout(Base):
    __tablename__ = "derived_workouts"

    user_id = Column(Text, primary_key=True)
    workout_uuid = Column(Text, primary_key=True)

    workout_type = Column(Text, nullable=True)
    start_ts = Column(DateTime(timezone=True), index=True, nullable=False)
    end_ts = Column(DateTime(timezone=True), nullable=True)

    duration_min = Column(Float, nullable=True)
    distance_km = Column(Float, nullable=True)
    energy_kcal = Column(Float, nullable=True)

    hk_source_bundle_id = Column(Text, nullable=True)
    hk_sources = Column(JSONB, nullable=True)
    hk_metadata = Column(JSONB, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class DerivedWorkoutSegment(Base):
    __tablename__ = "derived_workout_segments"

    user_id = Column(Text, primary_key=True)
    workout_uuid = Column(Text, primary_key=True)
    segment_unit = Column(Text, primary_key=True)
    segment_index = Column(Integer, primary_key=True)

    workout_start_ts = Column(DateTime(timezone=True), nullable=False)
    start_ts = Column(DateTime(timezone=True), nullable=False)
    end_ts = Column(DateTime(timezone=True), nullable=False)

    start_offset_min = Column(Float, nullable=False)
    end_offset_min = Column(Float, nullable=False)
    duration_min = Column(Float, nullable=False)

    pace_s_per_unit = Column(Float, nullable=True)
    avg_hr_bpm = Column(Float, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())


class DerivedSleepDaily(Base):
    __tablename__ = "derived_sleep_daily"

    user_id = Column(Text, primary_key=True)
    sleep_date = Column(Date, primary_key=True)

    sleep_start_ts = Column(DateTime(timezone=True), nullable=True)
    sleep_end_ts = Column(DateTime(timezone=True), nullable=True)

    asleep_minutes = Column(Float, nullable=True)
    rem_minutes = Column(Float, nullable=True)
    core_minutes = Column(Float, nullable=True)
    deep_minutes = Column(Float, nullable=True)
    awake_minutes = Column(Float, nullable=True)
    in_bed_minutes = Column(Float, nullable=True)
    asleep_unspecified_minutes = Column(Float, nullable=True)

    hk_sources = Column(JSONB, nullable=True)
    meta = Column(JSONB, nullable=True)



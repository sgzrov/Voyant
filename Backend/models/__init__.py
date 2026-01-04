# Import all SQLAlchemy models so Alembic autogenerate can discover tables via Base.metadata.
# Alembic's env.py imports this package for side effects.


# App-domain tables
from .chat_models import ChatConversation, ChatData  # noqa: F401
from .health_upload_tracking_model import HealthUploadTracking  # noqa: F401

# Health tables (main_* and derived_*)
from .health_models import (  # noqa: F401
    DerivedRollupDaily,
    DerivedRollupHourly,
    DerivedSleepDaily,
    DerivedWorkout,
    DerivedWorkoutSegment,
    MainHealthEvent,
    MainHealthMetric,
)



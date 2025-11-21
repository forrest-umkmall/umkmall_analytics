"""
Data models for Eduqat API responses.

These models document the structure of the Eduqat API and provide
type hints for working with the data in Python.
"""

from dataclasses import dataclass
from typing import Optional, Any
from datetime import datetime


@dataclass
class UserData:
    """User information embedded in enrollment."""
    id: str
    name: str
    email: str
    phone_number: Optional[str]
    subid: str
    description: Optional[str]
    role: str
    avatar_url: Optional[str]
    metadata: Optional[dict]
    status: str
    pre_signup_at: Optional[str]
    confirmed_at: Optional[str]
    last_loggin_at: Optional[str]
    created_at: str
    updated_at: str


@dataclass
class Certificate:
    """Certificate earned by user for course completion."""
    id: str
    enrollment_id: str
    user_id: str
    course_id: int
    course_certificate_id: int
    certificate_name: str
    certificate_template_id: str
    completed_at: str
    started_at: str
    expired_at: Optional[str]
    metadata: dict
    created_at: str
    updated_at: str


@dataclass
class SurveySubmission:
    """Survey or quiz submission within a material."""
    submission_id: str
    submitted_at: str
    survey_id: str
    survey_type: str  # 'quiz' or 'survey'
    # Quiz-specific fields (optional)
    correct_answer: Optional[int] = None
    wrong_answer: Optional[int] = None
    score: Optional[int] = None
    passing_grade: Optional[int] = None
    is_passed: Optional[bool] = None
    status: Optional[str] = None  # 'failed', 'passed'


@dataclass
class MaterialCompletion:
    """Completion status for a single material/lesson.

    The materials dict uses material IDs as keys (e.g., "2", "7", "39").
    """
    # Common fields
    type: Optional[str] = None  # 'video', 'text', 'rich_text', 'survey', 'quiz', 'self-assessment', 'ai_tutor', 'pdf', 'download'
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    is_required: Optional[bool] = None
    tracked_time: Optional[float] = None

    # Survey/Quiz specific
    submitted_at: Optional[str] = None
    survey_id: Optional[str] = None
    submission_id: Optional[str] = None
    survey_type: Optional[str] = None
    survey_submissions: Optional[dict] = None  # Dict of survey_id -> list of SurveySubmission

    # AI Tutor specific
    conversation_id: Optional[str] = None
    start_conversation: Optional[str] = None
    end_conversation: Optional[str] = None
    submission_conversation_id: Optional[int] = None
    count_submit: Optional[int] = None
    status: Optional[str] = None


@dataclass
class TrackedTime:
    """Time tracking information."""
    total: float
    last_tracked_at: str


@dataclass
class LastVisitedMaterial:
    """Information about the last material visited by user."""
    id: int
    type: str
    title: str
    status: str
    course_id: int
    menu_order: int
    session_id: int
    meta: dict  # Contains material-specific configuration


@dataclass
class EnrollmentMetadata:
    """Metadata for an enrollment."""
    type: str  # e.g., 'manual'
    timezone: str
    started_at: str  # This is the enrollment start date
    tracked_time: TrackedTime
    last_visited_material: Optional[LastVisitedMaterial]

    # Prakerja-related fields (Indonesian government program)
    redeem_at: Optional[str] = None
    prakerja_id: Optional[str] = None
    is_prakerja_user: Optional[bool] = None
    dummy_redeem_code: Optional[str] = None
    prakerja_redeem_at: Optional[str] = None
    prakerja_redeem_code: Optional[str] = None
    prakerja_invoice_code: Optional[str] = None
    prakerja_redeem_state: Optional[str] = None
    prakerja_redeem_code_data: Optional[dict] = None
    attendance_percentage: Optional[float] = None


@dataclass
class Completions:
    """Container for material completions."""
    materials: dict[str, MaterialCompletion]  # Key is material ID as string


@dataclass
class Enrollment:
    """
    Main enrollment object from /manage/admin/enrollments endpoint.

    Represents a user's enrollment in a course, including their progress,
    completions, and any earned certificates.
    """
    # Primary identifiers
    id: str
    uid: str
    user_id: str
    course_id: int
    price_id: int

    # Timestamps
    created_at: str
    completed_at: Optional[str]
    expires_at: Optional[str]

    # Progress metrics
    learning_progress: float  # 0-100 percentage
    learning_time: int  # Total time in seconds

    # Nested objects (stored as JSONB in raw table)
    user_data: UserData
    metadata: EnrollmentMetadata
    completions: Completions
    certificates: list[Certificate]

    # Additional fields
    timezone: str
    order_data: bool
    order_uid: Optional[str]
    schedule_id: Optional[int]
    user_groups: list
    user_group_admins: list


@dataclass
class EnrollmentsResponse:
    """Response from /manage/admin/enrollments endpoint."""
    count: int
    items: list[Enrollment]


# Helper functions for working with the data

def get_enrollment_start_date(enrollment: dict) -> Optional[str]:
    """Extract the enrollment start date from metadata.started_at."""
    metadata = enrollment.get('metadata', {})
    return metadata.get('started_at')


def get_first_material_start(enrollment: dict) -> Optional[str]:
    """Find the earliest started_at across all materials."""
    completions = enrollment.get('completions', {})
    materials = completions.get('materials', {})

    start_dates = []
    for material_id, material in materials.items():
        if isinstance(material, dict) and material.get('started_at'):
            start_dates.append(material['started_at'])

    if start_dates:
        return min(start_dates)
    return None


def get_total_tracked_time(enrollment: dict) -> Optional[float]:
    """Get total tracked time from metadata."""
    metadata = enrollment.get('metadata', {})
    tracked_time = metadata.get('tracked_time', {})
    return tracked_time.get('total')
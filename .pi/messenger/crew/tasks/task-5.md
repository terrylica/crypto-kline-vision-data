# Exception Hierarchy Documentation

Compare documented exceptions in src/CLAUDE.md and README.md with actual exception classes in utils/for_core/. Import each documented exception and verify it exists with correct attributes: RateLimitError and RestAPIError from rest_exceptions.py, VisionAPIError from vision_exceptions.py, streaming exceptions from streaming_exceptions.py. Check .details attribute exists on all exceptions. Broadcast missing, renamed, or incorrectly documented exceptions.

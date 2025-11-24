# CSV Upload Deduplication Solution

## Problem Statement
The backend was processing duplicate CSV uploads multiple times, leading to:
- Multiple Celery tasks for the same data
- Duplicate data insertion into the database
- Inefficient resource usage
- Potential data corruption

## Root Causes
1. **No idempotency mechanism**: Each upload request created a new task without checking for duplicates
2. **No content-based deduplication**: Same file uploaded multiple times was processed repeatedly
3. **No rate limiting**: Clients could send rapid duplicate requests
4. **No request tracking**: No visibility into upload history or duplicate attempts

## Solution Implemented

### 1. Database Schema Changes
Created `health_upload_tracking` table to track all uploads:
- **Primary key**: SHA256 hash of content (ensures uniqueness per content)
- **Tracking fields**: user_id, task_id, status, timestamps, request_count
- **Status management**: pending, processing, completed, failed, timeout

### 2. Content-Based Deduplication
- Generate SHA256 hash of uploaded CSV content
- Check if same content already uploaded/processing
- Return existing task_id if upload is in progress or completed
- Only create new task for truly new content or failed uploads

### 3. Idempotency Support
- Accept optional `X-Idempotency-Key` header from clients
- iOS client generates key based on content hash
- Prevents duplicate processing even with network retries

### 4. Rate Limiting
- In-memory rate limiter: max 10 uploads per minute per user
- Returns 429 status code when limit exceeded
- Provides wait time information to clients

### 5. Timeout Handling
- Automatically mark uploads as timed out after 5 minutes
- Allow reprocessing of timed-out uploads
- Prevent infinite waiting on stuck tasks

### 6. Request Tracking
- Track number of duplicate request attempts
- Maintain upload history with status
- Provide endpoints to query upload history
- Cleanup endpoint for old records

## API Changes

### Upload Endpoint Enhancement
```python
POST /health/upload-csv
Headers:
  X-Idempotency-Key: <optional-key>

Response:
{
  "task_id": "uuid",
  "status": "new|processing|completed|reprocessing",
  "message": "optional status message"
}
```

### New Endpoints
```python
GET /health/upload-history?limit=10
DELETE /health/cleanup-uploads?days_old=7
```

## Client Changes

### iOS Client Updates
- Generate SHA256 hash of CSV data
- Send hash as idempotency key
- Handle different upload statuses
- Log upload status for debugging

## Benefits
1. **Eliminates duplicate processing**: Same content never processed twice
2. **Reduces server load**: No redundant Celery tasks
3. **Improves reliability**: Handles retries and timeouts gracefully
4. **Better observability**: Track upload attempts and success rates
5. **Prevents abuse**: Rate limiting protects against rapid requests

## Testing
Use `test_upload_dedup.py` to verify:
- Concurrent uploads return same task_id
- Rate limiting works correctly
- Upload history tracks attempts
- Idempotency keys prevent duplicates

## Migration
Run the migration to create tracking table:
```bash
cd Backend
alembic upgrade head
```

## Monitoring
Monitor these metrics:
- Upload request_count per unique content
- Rate limit violations
- Timeout rates
- Failed upload rates

## Future Improvements
1. Redis-based rate limiting for multi-server deployments
2. Distributed lock for upload processing
3. Background job to auto-cleanup old records
4. Metrics dashboard for upload analytics
5. Configurable rate limits per user tier

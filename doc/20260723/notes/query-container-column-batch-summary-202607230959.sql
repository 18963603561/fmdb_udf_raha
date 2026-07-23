SELECT job_id, stage_type, status, summary_json
FROM dw.raha_job_stage_attempt
WHERE job_id IN (
    'job-148aec5c-b461-4399-a6ed-87eb27db61c8',
    'job-b953d1b9-a065-4d11-875b-8c135a1d43fd'
)
ORDER BY started_at;

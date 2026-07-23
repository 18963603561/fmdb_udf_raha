select stage_id, stage_type, status, error_code, started_at, completed_at,
       completed_at - started_at as elapsed_millis
from dw.raha_job_stage_attempt
where job_id = 'job-6e8b4790-8903-4815-b735-d8ab89cca04f'
order by started_at, attempt_id;

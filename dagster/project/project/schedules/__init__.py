from dagster import ScheduleDefinition

from ..jobs import salaries_job

salaries_schedule = ScheduleDefinition(job=salaries_job, cron_schedule="0 * * * *")

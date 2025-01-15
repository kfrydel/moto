import csv
import datetime
import io
import traceback

import time
import uuid
from enum import Enum
from threading import Thread
from typing import Any, Dict, List, Optional, Tuple

from moto.core.common_models import BaseModel
from moto.moto_api._internal.managed_state_model import ManagedState
from moto.s3.exceptions import MissingBucket

from .exceptions import InvalidJobOperation, ValidationError

restoration_delay_in_seconds = 60


def set_restoration_delay(value_in_seconds):
    global restoration_delay_in_seconds
    restoration_delay_in_seconds = value_in_seconds


def validate_job_status(target_job_status: str, valid_job_statuses: List[str]) -> None:
    if target_job_status not in valid_job_statuses:
        raise ValidationError(
            (
                "1 validation error detected: Value at 'current_status' failed "
                "to satisfy constraint: Member must satisfy enum value set: {valid_statues}"
            ).format(valid_statues=valid_job_statuses)
        )


# https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops-job-status.html
class JobStatus(str, Enum):
    ACTIVE = "Active"
    CANCELLED = "Cancelled"
    CANCELLING = "Cancelling"
    COMPLETE = "Complete"
    COMPLETING = "Completing"
    FAILED = "Failed"
    FAILING = "Failing"
    NEW = "New"
    PAUSED = "Paused"
    PAUSING = "Pausing"
    PREPARING = "Preparing"
    READY = "Ready"
    SUSPENDED = "Suspended"

    @classmethod
    def job_statuses(cls) -> List[str]:
        return sorted([item.value for item in JobStatus])

    @classmethod
    def status_transitions(cls) -> List[Tuple[Optional[str], str]]:
        return [
            (JobStatus.NEW.value, JobStatus.PREPARING.value),
            (JobStatus.PREPARING.value, JobStatus.SUSPENDED.value),
            (JobStatus.SUSPENDED.value, JobStatus.READY.value),
            (JobStatus.PREPARING.value, JobStatus.READY.value),
            (JobStatus.READY.value, JobStatus.ACTIVE.value),
            (JobStatus.PAUSING.value, JobStatus.PAUSED.value),
            (JobStatus.CANCELLING.value, JobStatus.CANCELLED.value),
            (JobStatus.FAILING.value, JobStatus.FAILED.value),
            (JobStatus.ACTIVE.value, JobStatus.COMPLETE.value),
        ]


class JobDefinition:
    def __init__(
        self,
        account_id: str,
        partition: str,
        operation_name: str,
        operation_definition: Dict[str, Any],
        params: Dict[str, Any],
        manifest_arn: str,
        manifest_etag: str | None,
        manifest_format: str,
        manifest_fields: str,
        report_bucket: str | None,
        report_enabled: bool,
        report_format: str | None,
        report_prefix: str | None,
        report_scope: str | None,
    ):
        self.account_id = account_id
        self.partition = partition
        self.operation_name = operation_name
        self.operation_definition = operation_definition
        self.params = params
        self.manifest_arn = manifest_arn
        self.manifest_etag = manifest_etag
        self.manifest_format = manifest_format
        self.manifest_fields = manifest_fields
        self.report_bucket = report_bucket
        self.report_enabled = report_enabled
        self.report_format = report_format
        self.report_prefix = report_prefix
        self.report_scope = report_scope

    @classmethod
    def from_dict(
        cls, account_id: str, partition: str, params: Dict[str, Any]
    ) -> "JobDefinition":
        operation_tuple = list(params["Operation"].items())[0]
        operation_name = operation_tuple[0]
        operation_params = operation_tuple[1]
        manifest = params["Manifest"]
        manifest_location = manifest["Location"]
        manifest_location_etag = manifest_location.get("ETag")
        manifest_location_object_arn = manifest_location.get("ObjectArn")
        manifest_spec = manifest["Spec"]
        manifest_fields = manifest_spec["Fields"]
        if isinstance(manifest_fields, dict) and "member" in manifest_fields:
            manifest_fields = manifest_fields["member"]
        manifest_format = manifest_spec["Format"]
        report = params["Report"]
        report_bucket = report.get("Bucket")
        report_enabled = report.get("Enabled")
        report_format = report.get("Format")
        report_prefix = report.get("Prefix")
        report_scope = report.get("ReportScope")
        operation_definition = params["Operation"]
        if report_enabled is not None:
            report_enabled = report_enabled.lower() == "true"
        return JobDefinition(
            account_id=account_id,
            partition=partition,
            operation_name=operation_name,
            operation_definition=operation_definition,
            params=operation_params,
            manifest_arn=manifest_location_object_arn,
            manifest_etag=manifest_location_etag,
            manifest_format=manifest_format,
            manifest_fields=manifest_fields,
            report_bucket=report_bucket,
            report_enabled=report_enabled,
            report_format=report_format,
            report_prefix=report_prefix,
            report_scope=report_scope,
        )

    @property
    def manifest_bucket_name(self):
        return self.manifest_arn.split("/")[0].split(":")[-1]

    @property
    def manifest_path(self):
        return "/".join(self.manifest_arn.split("/")[1:])


class Job(Thread, BaseModel, ManagedState):
    def __init__(self, job_id, definition: JobDefinition):
        Thread.__init__(self)
        ManagedState.__init__(
            self,
            "s3control::job",
            JobStatus.status_transitions(),
        )

        self.job_id = job_id
        self.definition = definition
        self.creation_time = datetime.datetime.now()
        self.failure_reasons = []
        self.stop = False
        self.number_of_tasks_succeeded = 0
        self.number_of_tasks_failed = 0
        self.total_number_of_tasks = 0
        self.elapsed_time_in_active_seconds = 0
        self._bucket_index_in_csv = self.definition.manifest_fields.index("Bucket")
        self._key_index_in_csv = self.definition.manifest_fields.index("Key")


class RestoreObjectJob(Job):
    def __init__(self, job_id, definition: JobDefinition):
        super().__init__(job_id, definition)
        self._expiration_days = 1
        if "ExpirationInDays" in self.definition.operation_definition:
            try:
                self._expiration_days = int(
                    self.definition.operation_definition["ExpirationInDays"]
                )
            except ValueError:
                raise ValidationError("ExpirationInDays")

    def run(self):
        try:
            self.status = JobStatus.PREPARING.value
            succeeded = []
            failed = []

            from moto.s3.models import s3_backends

            backend = s3_backends[self.definition.account_id][self.definition.partition]

            manifest_file_obj = None
            try:
                manifest_file_obj = backend.get_object(
                    self.definition.manifest_bucket_name, self.definition.manifest_path
                )  # type: ignore[attr-defined]
            except MissingBucket:
                pass

            if manifest_file_obj is None:
                self.failure_reasons.append(
                    {
                        "code": "ManifestNotFound",
                        "reason": "Manifest object was not found",
                    }
                )
                self.status = JobStatus.FAILED.value
                return

            self.status = JobStatus.ACTIVE.value

            expiration = datetime.datetime.now() + datetime.timedelta(
                self._expiration_days, restoration_delay_in_seconds
            )

            for bucket, key in self._buckets_and_keys_from_csv(manifest_file_obj):
                try:
                    key_obj = backend.get_object(bucket, key)
                except MissingBucket:
                    continue
                if key_obj is not None:
                    key_obj.status = "IN_PROGRESS"
                    key_obj.set_expiry(expiration)

            self.status = JobStatus.COMPLETE.value

            sleep_time = datetime.datetime.now() + datetime.timedelta(
                0, restoration_delay_in_seconds
            )
            while datetime.datetime.now() < sleep_time:
                time.sleep(0.5)
                if self.stop:
                    return

            for bucket_and_key in self._buckets_and_keys_from_csv(manifest_file_obj):
                try:
                    key_obj = backend.get_object(*bucket_and_key)
                except MissingBucket:
                    failed.append(bucket_and_key)
                    continue
                if key_obj is not None:
                    key_obj.restore(self._expiration_days)
                    succeeded.append(bucket_and_key)
                else:
                    failed.append(bucket_and_key)
        except Exception as exc:
            print(f"Exception in job {self.job_id}: {exc}\n")  # noqa: T201
            print(f"Stacktrace: {traceback.format_exc() }\n")  # noqa: T201

    def _buckets_and_keys_from_csv(self, file_obj):
        stream = io.StringIO(file_obj.value.decode(encoding="utf-8"))
        for row in csv.reader(stream):
            yield row[self._bucket_index_in_csv], row[self._key_index_in_csv]


operation_to_job = {"S3InitiateRestoreObject": RestoreObjectJob}


class JobsController:
    def __init__(self):
        self._jobs: Dict[str, Job] = {}

    def stop(self):
        for job in self._jobs.values():
            if job.status not in (JobStatus.FAILED, JobStatus.COMPLETE):
                job.stop = True
                # Try to join
                if job.is_alive():
                    job.join(2)

    def submit_job(
        self, account_id: str, partition: str, params: Dict[str, Any]
    ) -> str:
        job_definition = JobDefinition.from_dict(account_id, partition, params)
        job_class = operation_to_job.get(job_definition.operation_name)
        if job_class is None:
            raise InvalidJobOperation(
                f"Unsupported operation: {job_definition.operation_name}"
            )

        job_id = str(uuid.uuid4())
        job = job_class(job_id, job_definition)
        self._jobs[job_id] = job
        job.start()
        return job_id

    def get_job(self, job_id):
        return self._jobs.get(job_id)

#!/usr/bin/env python3.6
# pylint: disable=C0103
"""
Metering client for S3.
"""
from .client import Meterer


class S3Meterer(Meterer):
    """
    Meter an S3 bucket.
    """
    s3_url_prefix = "s3://"

    def __init__(self, cache, boto_session=None, cloudwatch_namespace=None):
        """
        S3Meterer(cache, boto_session=None, cloudwatch_namespace=None)
          -> S3Meterer

        Create a new S3Meterer, using the specified cache for recording access
        patterns and retrieving limits. If boto_session is not None, it is
        used to generate new S3 Boto clients. Otherwise, the default Boto3
        session is used.

        If cloudwatch_namespace is not None, CloudWatch metrics will be emitted
        to the specified namespace.
        """
        super(S3Meterer, self).__init__(cache)
        if boto_session is None:
            import boto3
            self.boto_session = boto3
        else:
            self.boto_session = boto_session

        self.cloudwatch_namespace = cloudwatch_namespace

        return

    def pool_for_resource(self, resource_name):
        """
        s3meterer.pool_for_resource(resource_name) -> str

        Returns the bucket for the given S3 resource, to be used as the
        metering pool. The S3 resource must be in the form 's3://bucket/key'.
        """
        return S3Meterer.resource_to_bucket_and_key(resource_name)[0]

    def get_actual_resource_size(self, resource_name):
        s3 = self.boto_session.resource("s3")
        bucket_name, key_name = S3Meterer.resource_to_bucket_and_key(
            resource_name)
        summary = s3.ObjectSummary(bucket_name, key_name)
        return summary.size


    @staticmethod
    def resource_to_bucket_and_key(resource_name):
        """
        S3Meterer.resource_to_bucket_and_key(resource_name) -> (str, str)

        Convert an S3 resource name in the form 's3://bucket/key' to a tuple
        of ('bucket', 'key').
        """
        if not resource_name.startswith(S3Meterer.s3_url_prefix):
            raise ValueError("Expected resource_name to start with %s" %
                             S3Meterer.s3_url_prefix)

        s3_bucket_and_key = resource_name[len(S3Meterer.s3_url_prefix):]

        try:
            bucket, key = s3_bucket_and_key.split("/", 1)
            if not bucket:
                raise ValueError("Bucket name cannot be empty in URL %r" %
                                 resource_name)

            return (bucket, key)
        except ValueError:
            raise ValueError("No key specified in URL %r" % resource_name)


    def log_attempt(self, pool, resource_name, resource_size):
        """
        Log an access attempt.
        """
        if self.cloudwatch_namespace:
            cw = self.boto_session.client("cloudwatch")
            cw.put_metric_data(
                Namespace=self.cloudwatch_namespace,
                MetricData={
                    "MetricName": "AccessAttempt",
                    "Dimensions": [
                        {
                            "Name": "Bucket",
                            "Value": pool,
                        }
                    ],
                    "Value": resource_size,
                    "Unit": "byte",
                }
            )

        return

    def log_attempt_hwm(self, pool, resource_name, period, period_str, aggregate_size):
        """
        Log the access attempt high water mark for a given time period.
        """
        if self.cloudwatch_namespace:
            cw = self.boto_session.client("cloudwatch")
            cw.put_metric_data(
                Namespace=self.cloudwatch_namespace,
                MetricData={
                    "MetricName": "AccessAttemptHWM",
                    "Dimensions": [
                        {
                            "Name": "Bucket",
                            "Value": pool,
                        },
                        {
                            "Name": "Period",
                            "Value": period,
                        },
                        {
                            "Name": "PeriodStamp",
                            "Value": period_str,
                        }
                    ],
                    "Value": aggregate_size,
                    "Unit": "byte",
                }
            )

    def log_allowance(self, pool, resource_name, resource_size):
        """
        Log an access allowance.
        """
        if self.cloudwatch_namespace:
            cw = self.boto_session.client("cloudwatch")
            cw.put_metric_data(
                Namespace=self.cloudwatch_namespace,
                MetricData={
                    "MetricName": "AccessAllowance",
                    "Dimensions": [
                        {
                            "Name": "Bucket",
                            "Value": pool,
                        }
                    ],
                    "Value": resource_size,
                    "Unit": "byte",
                }
            )

        return

    def log_allowance_hwm(self, pool, resource_name, period, period_str, aggregate_size):
        """
        Log the access allowance high water mark for a given time period. The
        default implementation is a no-op.
        """
        if self.cloudwatch_namespace:
            cw = self.boto_session.client("cloudwatch")
            cw.put_metric_data(
                Namespace=self.cloudwatch_namespace,
                MetricData={
                    "MetricName": "AccessAllowanceHWM",
                    "Dimensions": [
                        {
                            "Name": "Bucket",
                            "Value": pool,
                        },
                        {
                            "Name": "Period",
                            "Value": period,
                        },
                        {
                            "Name": "PeriodStamp",
                            "Value": period_str,
                        }
                    ],
                    "Value": aggregate_size,
                    "Unit": "byte",
                }
            )

        return

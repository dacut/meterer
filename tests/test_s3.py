#!/usr/bin/env python3
"""
Test the S3Meterer functionality.
"""
# pylint: disable=C0103,R0201
from datetime import datetime
from unittest import TestCase
import boto3
from moto import mock_s3
from .fake_cache import FakeCache


class TestS3Meterer(TestCase):
    """
    Test the S3Meterer.
    """

    @mock_s3
    def test_create(self):
        """
        Create an S3Meterer and do nothing else.
        """
        from meterer import S3Meterer
        S3Meterer(FakeCache())
        return

    @mock_s3
    def test_hourly_limit(self):
        """
        Create an S3 Meterer and ensure it refuses accesses in an hourly window.
        """
        from meterer import S3Meterer
        s3m = S3Meterer(FakeCache())
        s3m.set_limits_for_pool("bucketname", hour=100.0)

        when1 = datetime(
            year=2017, month=1, day=1, hour=0, minute=0, second=0)
        when2 = datetime(
            year=2017, month=1, day=1, hour=0, minute=59, second=59)
        when3 = datetime(
            year=2017, month=1, day=1, hour=1, minute=0, second=0)

        s3 = boto3.resource("s3")
        s3.Bucket("bucketname").create()
        k1 = s3.Object("bucketname", "key1")
        k1.put(Body=(b"\0" * 40))

        self.assertTrue(
            s3m.allow_resource_access("s3://bucketname/key1", when1))
        self.assertTrue(
            s3m.allow_resource_access("s3://bucketname/key1", when2))
        self.assertFalse(
            s3m.allow_resource_access("s3://bucketname/key1", when2))
        self.assertTrue(
            s3m.allow_resource_access("s3://bucketname/key1", when3))
        return

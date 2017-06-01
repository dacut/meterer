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
    def create_key(self, bucket="b1", key="k1", size=40):
        s3 = boto3.resource("s3")
        s3.Bucket(bucket).create()
        k1 = s3.Object(bucket, key)
        k1.put(Body=(b"\0" * 40))
        return

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
        s3m.set_limits_for_pool("b1", hour=100.0)
        self.assertEquals(s3m.get_limits_for_pool("b1"), {"hour": 100.0})
        self.assertEquals(
            s3m.get_period_strs(datetime(2017, 1, 1, 0, 0, 0)),
            {
                "year": "2017",
                "month": "2017-01",
                "day": "2017-01-01",
                "hour": "2017-01-01T00",
                "week": "2016-W52"
            })
        self.create_key()

        # Access #1 at 00:00: hour=40, ok
        self.assertTrue(s3m.allow_resource_access(
            "s3://b1/k1", datetime(2017, 1, 1, 0, 0, 0)))
        # Access #2 at 00:30: hour=80, ok
        self.assertTrue(s3m.allow_resource_access(
            "s3://b1/k1", datetime(2017, 1, 1, 0, 30, 0)))
        # Access #3 at 00:59: hour=80+40, fail
        self.assertFalse(s3m.allow_resource_access(
            "s3://b1/k1", datetime(2017, 1, 1, 0, 59, 0)))
        # Access #4 at 01:00: hour=40, ok
        self.assertTrue(s3m.allow_resource_access(
            "s3://b1/k1", datetime(2017, 1, 1, 1, 0, 0)))

        return

    @mock_s3
    def test_daily_limit(self):
        """
        Create an S3 Meterer and ensure it refuses accesses in a daily window.
        """
        from meterer import S3Meterer
        s3m = S3Meterer(FakeCache())
        s3m.set_limits_for_pool("b1", hour=100.0, day=300.0)
        self.assertEquals(
            s3m.get_limits_for_pool("b1"),
            {"hour": 100.0, "day": 300.0}
        )
        self.assertEquals(
            s3m.get_period_strs(datetime(2017, 1, 1, 0, 0, 0)),
            {
                "year": "2017",
                "month": "2017-01",
                "day": "2017-01-01",
                "hour": "2017-01-01T00",
                "week": "2016-W52"
            })
        self.create_key()

        # Access #1 at 00:00: hour=40, day=40, ok
        self.assertTrue(s3m.allow_resource_access(
            "s3://b1/k1", datetime(2017, 1, 1, 0, 0, 0)))
        # Access #2 at 00:30: hour=80, day=80, ok
        self.assertTrue(s3m.allow_resource_access(
            "s3://b1/k1", datetime(2017, 1, 1, 0, 30, 0)))
        # Access #3 at 00:59: hour=80+40, day=80+40, fail
        self.assertFalse(s3m.allow_resource_access(
            "s3://b1/k1", datetime(2017, 1, 1, 0, 59, 0)))
        # Access #4 at 01:00: hour=40, day=120, ok
        self.assertTrue(s3m.allow_resource_access(
            "s3://b1/k1", datetime(2017, 1, 1, 1, 0, 0)))
        # Access #5 at 01:30: hour=80, day=160, ok
        self.assertTrue(s3m.allow_resource_access(
            "s3://b1/k1", datetime(2017, 1, 1, 1, 30, 0)))
        # Access #6 at 01:59: hour=80+40, day=160+40, fail
        self.assertFalse(s3m.allow_resource_access(
            "s3://b1/k1", datetime(2017, 1, 1, 1, 30, 0)))
        # Access #4 at 02:00: hour=40, day=200, ok
        self.assertTrue(s3m.allow_resource_access(
            "s3://b1/k1", datetime(2017, 1, 1, 2, 0, 0)))
        # Access #4 at 03:00: hour=40, day=240, ok
        self.assertTrue(s3m.allow_resource_access(
            "s3://b1/k1", datetime(2017, 1, 1, 3, 0, 0)))
        # Access #5 at 04:00: hour=40, day=280, ok
        self.assertTrue(s3m.allow_resource_access(
            "s3://b1/k1", datetime(2017, 1, 1, 4, 0, 0)))
        # Access #5 at 04:30: hour=40+40, day=280+40, fail
        self.assertFalse(s3m.allow_resource_access(
            "s3://b1/k1", datetime(2017, 1, 1, 4, 30, 0)))

        return

    @mock_s3
    def test_bad_resource_names(self):
        """
        Create an S3 Meterer and ensure it refuses malformed resource names.
        """
        from meterer import S3Meterer
        s3m = S3Meterer(FakeCache())

        try:
            s3m.allow_resource_access("/foo/bar")
            self.fail("Expected ValueError")
        except ValueError:
            pass

        try:
            s3m.allow_resource_access("s3:///bar")
            self.fail("Expected ValueError")
        except ValueError:
            pass

        try:
            s3m.allow_resource_access("s3://foo")
            self.fail("Expected ValueError")
        except ValueError:
            pass

        return

    @mock_s3
    def test_alt_session(self):
        """
        Create an S3 Meterer and ensure it uses an alternative Boto3 session
        properly.
        """
        from meterer import S3Meterer
        from boto3.session import Session

        session = Session(region_name="us-west-2")

        s3m = S3Meterer(FakeCache(), session)
        self.create_key()
        self.assertTrue(s3m.allow_resource_access("s3://b1/k1"))
        return

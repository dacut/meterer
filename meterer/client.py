#!/usr/bin/env python3.6
"""
Generic metering client.
"""
import datetime
from json import dumps as json_dumps, loads as json_loads
from logging import getLogger
from time import time

log = getLogger("meterer.client")

class Meterer(object):
    """
    Meter resources over a given time period.

    A few basic concepts about the Meterer:
        * cache: The Meterer relies on a Redis cache to get and set data.
            This must be provided by the client.
        * resource: The underlying object being accessed. Every resource
            belongs to a pool. This is usually an n:1 mapping (one pool has
            many resources, e.g. an S3 bucket has many keys). The default
            implementation is a 1:1 mapping.
        * pool: This is what the Meterer is actually watching.
        * size: The size of a given resource. The units are arbitrary;
            typically, size means bytes (e.g. S3), but could be any scarce
            item (bandwidth, time, CPU utilisation, etc.).
    """

    # Allow overriding datetime for testing
    dt = datetime.datetime # pylint: disable=C0103

    # Timeout for caching resource sizes
    size_cache_timeout = 86400

    def __init__(self, cache):
        """
        Meterer(cache) -> Meterer

        Create a new Meterer, using the specified cache for recording access
        patterns and retrieving limits.
        """
        super(Meterer, self).__init__()
        self.cache = cache

        return

    def allow_resource_access(self, resource_name, when=None):
        """
        meterer.allow_resource_access(resource_name, when=None) -> bool

        Indicate whether access to the specified resource should be allowed.
        This checks the limit(s) for the pool associated with the resource and
        records the attempt. If the attempt falls below the limit, the allowance
        is also recorded and True is returned. Otherwise, False is returned.

        If when is None (the default), it defaults to the current time. This is
        used for testing.
        """
        # The period strings of interest to us
        period_strs = self.get_period_strs(when)

        # Get the pool and limits for the pool.
        pool = self.pool_for_resource(resource_name)
        limits = self.get_limits_for_pool(pool)

        resource_size = self.resource_size(resource_name)

        # Record the access attempt.
        for period, period_str in period_strs.items():
            aggregate_size = self.cache.incrbyfloat(
                "ATTEMPT:%s:%s" % (period_str, pool), resource_size)
            log.debug("allow_resource_access(%r): ATTEMPT:%s:%s=%s",
                      resource_name, period_str, pool, aggregate_size)

        del period_str

        # A list of things we need to undo from Redis if we breach a limit.
        undo_actions = []

        # Check for limit breaches.
        for period, period_str in period_strs.items():
            limit = limits.get(period)
            key = "ALLOWED:%s:%s" % (period_str, pool)
            result = self.cache.incrbyfloat(key, resource_size)
            undo_actions.append((key, -resource_size))
            log.debug("allow_resource_access(%r): ALLOWED:%s:%s=%s",
                      resource_name, period_str, pool, result)

            if limit and result > limit:
                log.debug("allow_resource_access(%r): Limit %s would be "
                          "breached: limit=%s, result=%s", resource_name,
                          period, limit, result)

                for key, incr in undo_actions:
                    self.cache.incrbyfloat(key, incr)

                return False

        log.debug("allow_resource_access(%r): No limits breached; allowed",
                  resource_name)
        return True

    def pool_for_resource(self, resource_name): # pylint: disable=R0201
        """
        meterer.pool_for_resource(resource_name) -> str

        Convert a resource name to the pool responsible for it. The default
        version just returns the resource name.
        """
        return resource_name

    def resource_size(self, resource_name): # pylint: disable=R0201
        """
        meterer.resource_size(resource_name) -> int

        Retrieve the size of the given resource.
        """
        cached_size = self.get_cached_resource_size(resource_name)
        if cached_size is not None:
            return cached_size["size"]

        # Not cached or cache timeout exceeded. Get the actual size and record
        # it in the cache.
        actual_size = self.get_actual_resource_size(resource_name)
        self.set_cached_resource_size(resource_name, actual_size)
        return actual_size

    def get_limits_for_pool(self, pool):
        """
        meterer.get_limits_for_pool(pool) -> dict

        Returns the limits for the given pool. If the pool does not have limits
        set, {} is returned.

        The resulting dict has the following format. Each item is optional and
        indicates no limit for the specified time period.
        {
            "year": int,
            "month": int,
            "week": int,
            "day": int,
            "hour": int,
        }
        """
        pool_limits = self.cache.get("LIMIT:%s" % pool)
        if pool_limits is None:
            return {}

        return json_loads(pool_limits)

    def set_limits_for_pool(self, pool, **kw):
        """
        meterer.set_limits_for_pool(pool, [time_period=value])

        Sets the limits for the given pool. The valid time_periods are:
        year, month, week, day, hour.
        """
        pool_limits = {}

        for time_period in ["year", "month", "week", "day", "hour"]:
            if time_period not in kw:
                continue

            limit = kw.pop(time_period)
            if limit is not None and not isinstance(limit, (float, int)):
                raise TypeError("%s must be a float or int or None",
                                time_period)
            pool_limits[time_period] = limit

        if kw:
            raise ValueError("Unknown time periods specified: %s" %
                             ", ".join(kw.keys()))

        self.cache.set("LIMIT:%s" % pool, json_dumps(pool_limits))
        return

    def get_cached_resource_size(self, resource_name):
        """
        meterer.get_cached_resource_size(resource_name) -> dict

        Retrieve cached information about the given resource. The resulting
        dict has the following structure:
        {
            "size": int,                # Size of the resource
            "recorded_time": float,     # Unix timestamp when this was generated
        }
        """
        size_data = self.cache.get("SIZE:%s" % resource_name)
        if size_data is None:
            return None

        return json_loads(size_data)

    def set_cached_resource_size(self, resource_name, size):
        """
        meterer.set_cached_resource_size(resource_name)

        Set cached information about the given resource.
        """
        size_data = json_dumps({"size": size, "recorded_time": time()})
        self.cache.set(
            "SIZE:%s" % resource_name, size_data,
            ex=self.size_cache_timeout)
        return

    def get_actual_resource_size(self, resource_name):
        """
        meterer.get_actual_resource_size(resource_name) -> int

        Returns the actual size of a given resource. This may be an expensive
        operation, so we rely heavily on cached data.
        """
        raise NotImplementedError(
            "get_actual_resource_size must be implemented by a subclass.")

    def get_period_strs(self, now=None):
        """
        meterer.get_period_strings()

        Return the period strings for the specified time (defaulting to the
        current time if not specified).
        """
        if now is None:
            now = self.dt.utcnow()

        year_str = "%04d" % now.year
        month_str = "%04d-%02d" % (now.year, now.month)
        day_str = "%04d-%02d-%02d" % (now.year, now.month, now.day)
        hour_str = "%04d-%02d-%02dT%02d" % (
            now.year, now.month, now.day, now.hour)

        # Note: The week number may be in the previous year during the last
        # days of the year.
        isocal = now.isocalendar()
        week_str = "%04d-W%02d" % (isocal[0], isocal[1])

        return {
            "year": year_str,
            "month": month_str,
            "day": day_str,
            "hour": hour_str,
            "week": week_str,
        }

"""A canonical calendar that can be joined with other tables to provide information about dates."""


from datetime import timedelta
import logging

import luigi
import luigi.configuration

from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util import Week
from edx.analytics.tasks.util.hive import HiveTableTask, HivePartitionTask
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

log = logging.getLogger(__name__)

MONTHS_PER_YEAR = 12
MONTHS_PER_QUARTER = MONTHS_PER_YEAR / 4


class CalendarDownstreamMixin(OverwriteOutputMixin):
    """The parameters needed to generate a complete calendar."""

    interval = luigi.DateIntervalParameter(
        default_from_config={'section': 'calendar', 'name': 'interval'}
    )


class CalendarTask(CalendarDownstreamMixin, luigi.Task):
    """
    Generate a canonical calendar.
    This table provides information about every day in every year that is being analyzed. It captures many complex
    details associated with calendars and standardizes references to concepts like "weeks" since they can be defined
    in different ways by various systems.
    It is also intended to contain business-specific metadata about dates in the future, such as fiscal year boundaries,
    fiscal quarter boundaries and even holidays or other days of special interest for analysis purposes.
    Parameters:
        output_root (str): path to store the calendar data
    """

    output_root = luigi.Parameter()

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'data.tsv'))

    def run(self):
        self.remove_output_on_overwrite()

        with self.output().open('w') as output_file:
            for date in self.interval:
                iso_year, iso_weekofyear, iso_weekday = date.isocalendar()
                week = Week(iso_year, iso_weekofyear)

                column_values = (
                    date.isoformat(),
                    date.year,
                    date.month,
                    date.day,
                    '{0:04d}W{1:02d}'.format(iso_year, iso_weekofyear),
                    week.monday().isoformat(),
                    (week.sunday() + timedelta(1)).isoformat(),
                    iso_weekday
                )
                output_file.write('\t'.join([unicode(v).encode('utf8') for v in column_values]) + '\n')


class CalendarTableTask(HiveTableTask):
    """Ensure a hive table exists for the calendar so that we can perform joins."""

    @property
    def table(self):
        return 'calendar'

    @property
    def columns(self):
        return [
            ('date', 'STRING'),
            ('year', 'INT'),
            ('month', 'INT'),
            ('day', 'INT'),
            ('iso_weekofyear', 'STRING'),
            ('iso_week_start', 'STRING'),
            ('iso_week_end', 'STRING'),
            ('iso_weekday', 'INT'),
        ]


class CalendarPartitionTask(CalendarDownstreamMixin, HivePartitionTask):

    @property
    def hive_table_task(self):
        return CalendarTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    @property
    def partition_value(self):
        return self.interval.date_b.isoformat()

    def requires(self):
        yield self.hive_table_task
        yield CalendarTask(
            output_root=self.partition_location,
            interval=self.interval,
            overwrite=self.overwrite,
        )

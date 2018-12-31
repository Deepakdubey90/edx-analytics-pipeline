"""
Loads multiple insights tables into the warehouse through the pipeline via Hive.
"""
import datetime
import logging

import luigi

from edx.analytics.tasks.common.vertica_load import VerticaCopyTask, VerticaCopyTaskMixin
from edx.analytics.tasks.enterprise.enterprise_enrollments import EnterpriseEnrollmentRecord
from edx.analytics.tasks.enterprise.enterprise_user import EnterpriseUserRecord
from edx.analytics.tasks.insights.answer_dist import AnswerDistributionRecord
from edx.analytics.tasks.insights.enrollments import EnrollmentDailyRecord, EnrollmentByBirthYearRecord, \
    EnrollmentByEducationLevelRecord, EnrollmentByGenderRecord, EnrollmentByModeRecord, CourseSummaryEnrollmentRecord, \
    CourseProgramMetadataRecord
from edx.analytics.tasks.insights.location_per_course import LastCountryOfUserRecord, LastCountryPerCourseRecord
from edx.analytics.tasks.insights.module_engagement import ModuleEngagementRecord, \
    ModuleEngagementSummaryMetricRangeRecord
from edx.analytics.tasks.insights.user_activity import CourseActivityRecord
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.util.url import ExternalURL

log = logging.getLogger(__name__)


class LoadHiveTableToVertica(WarehouseMixin, VerticaCopyTask):
    """
    Generic task to load hive table into Vertica.
    """
    date = luigi.DateParameter()
    table_name = luigi.Parameter(
        description='Name of hive table to load into vertica.'
    )
    column_list = luigi.ListParameter(
        description='A list of column names to be included.'
    )

    @property
    def insert_source_task(self):
        return ExternalURL(url=self.hive_partition_path(self.table_name, self.date))

    @property
    def table(self):
        return self.table_name

    @property
    def default_columns(self):
        return None

    @property
    def auto_primary_key(self):
        return None

    @property
    def columns(self):
        return self.column_list


class LoadInsightsTableToVertica(WarehouseMixin, VerticaCopyTaskMixin, luigi.WrapperTask):
    """Wrapper task to insert data into Vertica."""

    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Default is today, UTC.',
    )
    schema = luigi.Parameter(
        default='insights',
        description='The schema to which to write.',
    )

    def requires(self):
        kwargs = {
            'date': self.date,
            'warehouse_path': self.warehouse_path,
            'overwrite': True,
            'schema': self.schema,
            'credentials': self.credentials,
            'read_timeout': self.read_timeout,
            'marker_schema': self.marker_schema,
        }
        yield (
            LoadHiveTableToVertica(
                table_name='answer_distribution',
                column_list=AnswerDistributionRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='course_activity',
                column_list=CourseActivityRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='course_enrollment_daily',
                column_list=EnrollmentDailyRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='course_enrollment_birth_year_daily',
                column_list=EnrollmentByBirthYearRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='course_enrollment_education_level_daily',
                column_list=EnrollmentByEducationLevelRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='course_enrollment_gender_daily',
                column_list=EnrollmentByGenderRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='course_enrollment_mode_daily',
                column_list=EnrollmentByModeRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='course_meta_summary_enrollment',
                column_list=CourseSummaryEnrollmentRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='course_program_metadata',
                column_list=CourseProgramMetadataRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='course_enrollment_location_current',
                column_list=LastCountryPerCourseRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='last_country_of_user',
                column_list=LastCountryOfUserRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='module_engagement',
                column_list=ModuleEngagementRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='module_engagement_metric_ranges',
                column_list=ModuleEngagementSummaryMetricRangeRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='enterprise_enrollment',
                column_list=EnterpriseEnrollmentRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='enterprise_user',
                column_list=EnterpriseUserRecord.get_sql_schema(),
                **kwargs
            )
        )

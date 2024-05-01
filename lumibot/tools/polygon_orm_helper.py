from datetime import datetime
import os
import pytz

import polygon 

from polygon.rest.models import GroupedDailyAgg

import sqlalchemy
import sqlalchemy.orm


ORM_PRICE_PRECISION = 5


class UnixTimestampTZType(sqlalchemy.types.TypeDecorator):
    """Converts between Unix timestamp in milliseconds and PostgreSQL TIMESTAMPTZ."""
    impl = sqlalchemy.types.DateTime(timezone=True)

    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            # Convert Unix timestamp in milliseconds to a timezone-aware datetime object
            return datetime.datetime.fromtimestamp(value / 1000, tz=pytz.UTC)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            # Convert timezone-aware datetime object to Unix timestamp in milliseconds
            return int(value.timestamp() * 1000)
        return value


class UnixTimestampDateType(sqlalchemy.types.TypeDecorator):
    """Converts between Unix timestamp in milliseconds and Date."""
    impl = sqlalchemy.types.Date(timezone=True)

    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            # Convert Unix timestamp in milliseconds to a timezone-aware datetime object
            return datetime.fromtimestamp(value / 1000, tz=pytz.UTC)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            # Convert timezone-aware datetime object to Unix timestamp in milliseconds
            return int(value.timestamp() * 1000)
        return value


def polygon_orm_mapping(metadata: sqlalchemy.MetaData, mapper_registry: sqlalchemy.orm.registry):
    # Define SQLAlchemy table metadata with a custom column for timestamp
    grouped_daily_agg_table = sqlalchemy.Table(
        "grouped_daily_agg_us_stocks_adjusted", metadata,
        sqlalchemy.Column('timestamp', UnixTimestampDateType, primary_key=True, name='date'),
        sqlalchemy.Column('ticker', sqlalchemy.String, primary_key=True),
        sqlalchemy.Column('open', sqlalchemy.Float(precision=ORM_PRICE_PRECISION)),
        sqlalchemy.Column('high', sqlalchemy.Float(precision=ORM_PRICE_PRECISION)),
        sqlalchemy.Column('low', sqlalchemy.Float(precision=ORM_PRICE_PRECISION)),
        sqlalchemy.Column('close', sqlalchemy.Float(precision=ORM_PRICE_PRECISION)),
        sqlalchemy.Column('volume', sqlalchemy.Float),
        sqlalchemy.Column('vwap', sqlalchemy.Float(precision=ORM_PRICE_PRECISION)),
        sqlalchemy.Column('transactions', sqlalchemy.Integer),
        sqlalchemy.Column('otc', sqlalchemy.Boolean)
    )

    mapper_registry.map_imperatively(GroupedDailyAgg, grouped_daily_agg_table)


def initialize_polygon_orm(engine: sqlalchemy.engine.Engine):
    """Initialize the ORM for Polygon.io data."""
    metadata = sqlalchemy.MetaData()
    mapper_registry = sqlalchemy.orm.registry()
    polygon_orm_mapping(metadata, mapper_registry)

    # Create tables if they do not exist
    metadata.create_all(engine)



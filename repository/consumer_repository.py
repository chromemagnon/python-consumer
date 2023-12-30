
import logging
import os
from sqlalchemy import Column, create_engine, DateTime, Float, Integer, MetaData, select, String, Table
from sqlalchemy.exc import SQLAlchemyError


class ConsumerRepository:
    """
    Repository for handling storage of IoT device measurements.
    """
    def __init__(self):
        self.engine = create_engine(os.getenv('DATABASE_URL'))
        self.metadata = MetaData()

        self.iot_device_table = Table('iot_device', self.metadata,
                                      Column('id', Integer, primary_key=True),
                                      Column('device_identifier', String(45)))

        self.iot_measurement_table = Table('iot_measurement', self.metadata,
                                           Column('id', Integer, primary_key=True),
                                           Column('device_id', Integer),
                                           Column('value', Float),
                                           Column('created_date', DateTime))

        self.metadata.create_all(self.engine)

    def store_measurement(self, dto):
        """
        Stores a single IoT device measurement in the database.
        """
        try:
            with self.engine.connect() as connection:
                primary_key = self.get_primary_key(connection, dto.device_identifier)
                logging.info(connection)
                if primary_key is None:
                    logging.warning(f"Device ID not found for identifier: {dto.device_identifier}")
                    return

                insert_stmt = self.iot_measurement_table.insert().values(
                    device_id=primary_key,
                    value=dto.temperature,
                    created_date=dto.measurement_time
                )
                connection.execute(insert_stmt)
                connection.commit()
        except SQLAlchemyError as e:
            logging.error(f"Database Error: {e}")
            raise

    def get_primary_key(self, connection, device_identifier):
        """
        Retrieves the primary key of an IoT device based on its identifier.
        """
        select_stmt = select(self.iot_device_table.c.id).where(
            self.iot_device_table.c.device_identifier == str(device_identifier)
        )
        result = connection.execute(select_stmt).fetchone()
        return result[0] if result else None


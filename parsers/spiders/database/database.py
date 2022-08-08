import connector as CN
from sqlalchemy import Column, Identity
from sqlalchemy import String, Integer
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Indicator(Base):
    __tablename__ = "stg_indicators"

    record_id = Column(Integer, Identity(start=1), primary_key=True)
    indicator_name = Column(String(255))
    category = Column(String(255))
    frequency = Column(String(255))
    start_date = Column(String(255))
    measure_units = Column(String(255))
    value = Column(String(255))

    def __repr__(self):
        return f'''{self.record_id}, {self.indicator_name}, {self.category},
                    {self.frequency}, {self.start_date}, {self.measure_units}, {self.value}'''

engine = CN.Connector().connect()
Base.metadata.create_all(engine)

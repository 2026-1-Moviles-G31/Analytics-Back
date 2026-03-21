from sqlalchemy import Column, Integer, String, Boolean, BigInteger
from database import Base

class Event(Base):
    __tablename__ = "events"

    id          = Column(Integer, primary_key=True, index=True)
    event       = Column(String)
    page        = Column(String)
    feature     = Column(String, nullable=True)
    session     = Column(String)
    type        = Column(String)
    userID      = Column(String)
    milliseconds_spent = Column(Integer)
    timestamp   = Column(BigInteger)
    recovered   = Column(Boolean, default=False)
from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, create_engine
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()

class Company(Base):
    __tablename__ = "companies"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    ticker = Column(String)
    cik = Column(String)
    sector = Column(String)
    country = Column(String)

class Fund(Base):
    __tablename__ = "funds"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    type = Column(String)

class Holding(Base):
    __tablename__ = "holdings"
    id = Column(Integer, primary_key=True)
    date = Column(Date)
    fund_id = Column(Integer, ForeignKey("funds.id"))
    company_id = Column(Integer, ForeignKey("companies.id"))
    pos_usd = Column(Float)
    source = Column(String)

class Award(Base):
    __tablename__ = "awards"
    id = Column(Integer, primary_key=True)
    date = Column(Date)
    agency = Column(String)
    recipient = Column(String)
    company_id = Column(Integer, ForeignKey("companies.id"))
    amount_usd = Column(Float)
    program = Column(String)
    source = Column(String)

class Patent(Base):
    __tablename__ = "patents"
    id = Column(Integer, primary_key=True)
    pub_date = Column(Date)
    company_id = Column(Integer, ForeignKey("companies.id"))
    assignee = Column(String)
    title = Column(String)
    keywords = Column(String)
    url = Column(String)

class ETFTrade(Base):
    __tablename__ = "etf_trades"
    id = Column(Integer, primary_key=True)
    date = Column(Date)
    etf = Column(String)
    ticker = Column(String)
    direction = Column(String)
    value_usd = Column(Float)
    source = Column(String)

# Session factory
engine = create_engine("sqlite:///insti_flow.db")
SessionLocal = sessionmaker(bind=engine)
Base.metadata.create_all(bind=engine)

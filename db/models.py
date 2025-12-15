"""Database structure definitions using SQLAlchemy ORM."""

from sqlalchemy import Column, String, Float, DateTime, Integer, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

created_at = Column(DateTime(timezone=True), server_default=func.now())
updated_at = Column(DateTime(timezone=True), onupdate=func.now())

Base = declarative_base()

# List of tokens that the bot can trade.
class Token(Base):
    __tablename__ = "tokens"
    token_address = Column(String, primary_key=True, unique=True)
    pool_address = Column(String, unique=True)
    token_name = Column(String)
    liqd = Column(Float)
    age_days = Column(Integer)
    holders_count = Column(Integer)
    gt_score = Column(Float)


# List of tokens that are blacklisted from trading (at the filtering level).
class BlacklistedToken(Base):
    __tablename__ = "blacklisted_tokens"
    pool_address = Column(String, primary_key=True)
    reason = Column(String, nullable=True)  # можно указывать причину блокировки (опционально)
    added_at = Column(DateTime(timezone=True), server_default=func.now())


# List of tokens that the bot currently holds (bought but not sold).
class Hold(Base):
    __tablename__ = "holds"
    id = Column(Integer, primary_key=True)
    token_address = Column(String, ForeignKey("tokens.token_address"))
    pool_address  = Column(String, ForeignKey("tokens.pool_address"), unique=True)
    prob = Column(Float)
    buy_time = Column(DateTime(timezone=True))
    buy_price = Column(Float)
    now_price = Column(Float)
    percent_diff = Column(Float)
    amount = Column(Float)


# Logs of model predictions.
class PredictLog(Base):
    __tablename__ = "predict_logs"
    id = Column(Integer, primary_key=True)
    token_address = Column(String)
    pool_address = Column(String)
    time = Column(DateTime(timezone=True))
    prob = Column(Float)
    threshold = Column(Float)


# Logs of all transactions (buys and sells).
class TransactionLog(Base):
    __tablename__ = "transaction_logs"
    id = Column(Integer, primary_key=True)
    trade_type = Column(String)  # buy/sell
    timestamp = Column(DateTime(timezone=True))
    token_address = Column(String)
    pool_address = Column(String)
    buy_hesh = Column(String, unique=True)
    sell_hesh = Column(String, unique=True)
    buy_time = Column(DateTime(timezone=True))
    buy_price = Column(Float)
    sell_time = Column(DateTime(timezone=True))
    sell_price = Column(Float)
    profit = Column(Float)
    prob = Column(Float)
    threshold = Column(Float)
    buy_amount = Column(Float)
    sell_amount = Column(Float)


# Table with historical token prices.
class Prices(Base):
    __tablename__ = "prices"
    id = Column(Integer, primary_key=True)
    token_address = Column(String, ForeignKey("tokens.token_address"))
    pool_address = Column(String, ForeignKey("tokens.pool_address"))
    price = Column(Float)
    timestamp = Column(DateTime(timezone=True))


# Table with OHLCV data (custom).
class OHLCV(Base):
    __tablename__ = "ohlcv"
    id = Column(Integer, primary_key=True)
    token_address = Column(String, ForeignKey("tokens.token_address"))
    pool_address = Column(String, ForeignKey("tokens.pool_address"))
    timestamp = Column(DateTime(timezone=True))
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    __table_args__ = (
        UniqueConstraint('token_address', 'timestamp', name='uix_ohlcv_token_time'),
    )


# Table with OHLCV data from an external API (GeckoTerminal).
class OHLCV_API(Base):
    __tablename__ = "ohlcv_api"
    id = Column(Integer, primary_key=True)
    token_address = Column(String)
    pool_address = Column(String)
    timestamp = Column(DateTime(timezone=True))
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    __table_args__ = (
        UniqueConstraint('token_address', 'timestamp', name='uix_ohlcv_token_time_2'),
    )


class ParsedTokens(Base):
    __tablename__ = "parsed_tokens"
    id           = Column(Integer, primary_key=True)
    token_address = Column(String)
    pool_address = Column(String,  unique=True, index=True, nullable=False)
    token_name   = Column(String)
    liqd         = Column(Float) 
    age_days    = Column(Integer) 
    volume_24h   = Column(Float)
    holders_count = Column(Integer)
    gt_score     = Column(Float)

    __table_args__ = (
        UniqueConstraint("pool_address", name="uq_parsed_tokens_pool_address"),
    )
from typing import List
import databases
import sqlalchemy
from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
from sqlalchemy import BigInteger
from db import DATABASE_URL, table
# import asyncpg_simpleorm
# from asyncpg_simpleorm import async_model


database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()
# TODO asyncpg.exceptions.DataError: invalid input for query argument $1: 3013468463 (value out of int32 range)
leads = sqlalchemy.Table(
    table,
    metadata,
    sqlalchemy.Column("id", sqlalchemy.BigInteger, primary_key=True, unique=True),
    sqlalchemy.Column("tweet_text", sqlalchemy.String),
    sqlalchemy.Column("author_id", sqlalchemy.BigInteger),
    sqlalchemy.Column("lang", sqlalchemy.String),
    sqlalchemy.Column("created_at", sqlalchemy.DateTime, nullable=False),
    sqlalchemy.Column("matching_rules", sqlalchemy.String, nullable=True, ),
    sqlalchemy.Column("spark_timestamp", sqlalchemy.DateTime, nullable=False),
    sqlalchemy.Column("raw_json", sqlalchemy.String, nullable=False),
    sqlalchemy.Column("source", sqlalchemy.String),
    sqlalchemy.Column("tweet_count", sqlalchemy.String),
    sqlalchemy.Column("followers_count", sqlalchemy.String),
    sqlalchemy.Column("verified", sqlalchemy.String),
    sqlalchemy.Column("author_name", sqlalchemy.String),
    sqlalchemy.Column("author_username", sqlalchemy.String),
    sqlalchemy.Column("geo_type", sqlalchemy.String),
    sqlalchemy.Column("geo_name", sqlalchemy.String),
    sqlalchemy.Column("geo_country", sqlalchemy.String),
    sqlalchemy.Column("geo_full_name", sqlalchemy.String),
    sqlalchemy.Column("geo_place_type", sqlalchemy.String),
    sqlalchemy.Column("geo_country_code", sqlalchemy.String),
    sqlalchemy.Column("geo_bbox", sqlalchemy.String),
    sqlalchemy.Column("like_count", sqlalchemy.Integer),
    sqlalchemy.Column("quote_count", sqlalchemy.Integer),
    sqlalchemy.Column("reply_count", sqlalchemy.Integer),
    sqlalchemy.Column("retweet_count", sqlalchemy.Integer),
    sqlalchemy.Column("tweet_place_id", sqlalchemy.String),
    sqlalchemy.Column("coordinates_type", sqlalchemy.String),
    sqlalchemy.Column("coordinates", sqlalchemy.String),
)

engine = sqlalchemy.create_engine(
    DATABASE_URL, pool_size=21, max_overflow=0
)
metadata.create_all(engine)


class AddLead(BaseModel):
    tweet_text: str
    author_id: int
    lang: str
    created_at: datetime
    matching_rules: str
    spark_timestamp: datetime
    raw_json: str
    source: str
    tweet_count: str
    followers_count: str
    verified: str
    author_name: str
    author_username: str
    geo_type: str
    geo_name: str
    geo_country: str
    geo_full_name: str
    geo_place_type: str
    geo_country_code: str
    geo_bbox: str
    like_count: int
    quote_count: int
    reply_count: int
    retweet_count: int
    tweet_place_id: str
    coordinates_type: str
    coordinates: str


class Lead(BaseModel):
    id: int
    tweet_text: str
    author_id: int
    lang: str
    created_at: datetime
    matching_rules: str
    spark_timestamp: datetime
    raw_json: str
    source: str
    # tweet_count: str
    # followers_count: str
    # verified: str
    # author_name: str
    # author_username: str
    # geo_type: str
    # geo_name: str
    # geo_country: str
    # geo_full_name: str
    # geo_place_type: str
    # geo_country_code: str
    # geo_bbox: str
    # like_count: int
    # quote_count: int
    # reply_count: int
    # retweet_count: int
    # tweet_place_id: str
    # coordinates_type: str
    # coordinates: str


app = FastAPI(title="REST API using FastAPI PostgreSQL Async EndPoints")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.get("/lead/", response_model=List[Lead], status_code=status.HTTP_200_OK)
async def read_leads(skip: int = 0, take: int = 20):
    query = leads.select().offset(skip).limit(take)
    return await database.fetch_all(query)


@app.get("/lead/{lead_id""}/", response_model=Lead, status_code=status.HTTP_200_OK)
async def read_leads(lead_id: int):
    query = leads.select().where(leads.c.id == lead_id)
    return await database.fetch_one(query)


@app.post("/lead/", response_model=Lead, status_code=status.HTTP_201_CREATED)
async def create_lead(lead_dic: AddLead):
    query = leads.insert().values(tweet_text=lead_dic.tweet_text, author_id=lead_dic.author_id,
                                  lang=lead_dic.lang, created_at=lead_dic.created_at,
                                  matching_rules=lead_dic.matching_rules, spark_timestamp=lead_dic.spark_timestamp,
                                  raw_json=lead_dic.raw_json, source=lead_dic.source, tweet_count=lead_dic.tweet_count,
                                  followers_count=lead_dic.followers_count, verified=lead_dic.verified,
                                  author_name=lead_dic.author_name, author_username=lead_dic.author_username,
                                  geo_type=lead_dic.geo_type, geo_name=lead_dic.geo_name,
                                  geo_country=lead_dic.geo_country, geo_full_name=lead_dic.geo_full_name,
                                  geo_place_type=lead_dic.geo_place_type, geo_country_code=lead_dic.geo_country_code,
                                  geo_bbox=lead_dic.geo_bbox, like_count=lead_dic.like_count,
                                  quote_count=lead_dic.quote_count, reply_count=lead_dic.reply_count,
                                  retweet_count=lead_dic.retweet_count, tweet_place_id=lead_dic.tweet_place_id,
                                  coordinates_type=lead_dic.coordinates_type, coordinates=lead_dic.coordinates,
                                  )
    last_record_id = await database.execute(query)
    return {**lead_dic.dict(), "id": last_record_id}

# @app.put("/lead/{lead_id}/", response_model=Lead, status_code=status.HTTP_200_OK)
# async def update_lead(lead_id: int, payload: AddLead):
#     query = leads.update().where(leads.c.id == lead_id).values(text=payload.text, completed=payload.completed)
#     await database.execute(query)
#     return {**payload.dict(), "id": lead_id}
#
#
# @app.delete("/lead/{lead_id}/", status_code=status.HTTP_200_OK)
# async def delete_lead(lead_id: int):
#     query = leads.delete().where(leads.c.id == lead_id)
#     await database.execute(query)
#     return {"message": "Note with id: {} deleted successfully!".format(lead_id)}
#

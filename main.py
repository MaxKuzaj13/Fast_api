import databases
import sqlalchemy
from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional, Dict
from fastapi import Depends, FastAPI
from fastapi.security import HTTPBearer
from db import DATABASE_URL, table, API_KEY


database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()
leads = sqlalchemy.Table(
    table,
    metadata,
    sqlalchemy.Column("id", sqlalchemy.BigInteger, primary_key=True, unique=True),
    sqlalchemy.Column("tweet_text", sqlalchemy.String),
    sqlalchemy.Column("author_id", sqlalchemy.String),
    sqlalchemy.Column("lang", sqlalchemy.String),
    sqlalchemy.Column("created_at", sqlalchemy.DateTime),
    sqlalchemy.Column("matching_rules", sqlalchemy.String),
    sqlalchemy.Column("spark_timestamp", sqlalchemy.DateTime),
    sqlalchemy.Column("raw_json", sqlalchemy.String),
    sqlalchemy.Column("source", sqlalchemy.String),
    sqlalchemy.Column("tweet_count", sqlalchemy.String),
    sqlalchemy.Column("followers_count", sqlalchemy.Integer),
    sqlalchemy.Column("verified", sqlalchemy.BOOLEAN),
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
    sqlalchemy.Column("gls", sqlalchemy.BOOLEAN),
)

engine = sqlalchemy.create_engine(
    DATABASE_URL, pool_size=10, max_overflow=0
)
metadata.create_all(engine)


class Lead(BaseModel):
    id: Optional[str]
    tweet_text: Optional[str]
    author_id: Optional[str]
    lang: Optional[str]
    created_at: Optional[datetime]
    matching_rules: Optional[list]
    spark_timestamp: Optional[datetime]
    raw_json: Optional[str]
    source: Optional[str]
    # tweet_count: Optional[list]
    # followers_count: Optional[list]
    # verified: Optional[list]
    # author_name: Optional[list]
    # author_username: Optional[list]
    # geo_type: Optional[str]
    # geo_name: Optional[str]
    # geo_country: Optional[str]
    # geo_full_name: Optional[str]
    # geo_place_type: Optional[str]
    # geo_country_code: Optional[str]
    # geo_bbox: Optional[list]
    # like_count: Optional[int]
    # quote_count: Optional[int]
    # reply_count: Optional[int]
    # retweet_count: Optional[int]
    # tweet_place_id: Optional[str]
    # coordinates_type: Optional[str]
    # coordinates: Optional[list]
    gls: Optional[bool]


class AddLead(BaseModel):
    tweet_text: Optional[str]
    author_id: Optional[str]
    lang: Optional[str]
    created_at: Optional[datetime]
    matching_rules: Optional[list]
    spark_timestamp: Optional[datetime]
    raw_json: Optional[str]
    source: Optional[str]
    tweet_count: Optional[list]
    followers_count: Optional[list]
    verified: Optional[list]
    author_name: Optional[list]
    author_username: Optional[list]
    geo_type: Optional[str]
    geo_name: Optional[str]
    geo_country: Optional[str]
    geo_full_name: Optional[str]
    geo_place_type: Optional[str]
    geo_country_code: Optional[str]
    geo_bbox: Optional[list]
    like_count: Optional[int]
    quote_count: Optional[int]
    reply_count: Optional[int]
    retweet_count: Optional[int]
    tweet_place_id: Optional[str]
    coordinates_type: Optional[str]
    coordinates: Optional[list]
    gls: Optional[bool]

app = FastAPI(title="REST API using FastAPI PostgreSQL Async EndPoints")

token_auth_scheme = HTTPBearer()


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
async def read_leads(skip: int = 0, take: int | None = None, token: str = Depends(token_auth_scheme),
                     last_day: int | None = 0, last_minutes: int | None = 0 ):
    if token.credentials == API_KEY:
        if take:
            query = leads.select().offset(skip).limit(take)
        else:
            now = datetime.now()
            start_date = now - timedelta(days=last_day) - timedelta(minutes=last_minutes)
            query = leads.select().where(leads.c.created_at > start_date)
        return await database.fetch_all(query)
    else:
        return status.HTTP_401_UNAUTHORIZED


@app.get("/lead/{lead_id""}/", response_model=Lead, status_code=status.HTTP_200_OK)
async def read_leads(lead_id: str, token: str = Depends(token_auth_scheme)):
    if token.credentials == API_KEY:
        query = leads.select().where(leads.c.id == lead_id)
        return await database.fetch_one(query)
    else:
        return status.HTTP_401_UNAUTHORIZED


@app.post("/lead/add/", response_model=Lead, status_code=status.HTTP_201_CREATED)
async def create_lead(lead_dic: AddLead, token: str = Depends(token_auth_scheme)):
    if token.credentials == API_KEY:
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
                                      gls=lead_dic.gls
                                      )
        last_record_id = await database.execute(query)
        return {**lead_dic.dict(), "id": last_record_id}
    else:
        return status.HTTP_401_UNAUTHORIZED


@app.put("/lead/update/{lead_id}/", response_model=Lead, status_code=status.HTTP_200_OK)
async def update_lead(lead_id: int, update: AddLead, token: str = Depends(token_auth_scheme)):
    if token.credentials == API_KEY:
        query = leads.update().where(leads.c.id == str(lead_id)).values(gls=update.gls)
        await database.execute(query)
        return {**update.dict(), "id": lead_id}
    else:
        return status.HTTP_401_UNAUTHORIZED
#
#
# @app.delete("/lead/{lead_id}/", status_code=status.HTTP_200_OK)
# async def delete_lead(lead_id: int):
#     query = leads.delete().where(leads.c.id == lead_id)
#     await database.execute(query)
#     return {"message": "Note with id: {} deleted successfully!".format(lead_id)}
#

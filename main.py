from sqlalchemy import Table, Column, Float, String, MetaData, create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

engine = create_engine('sqlite:///database.db', echo=True)  
meta = MetaData()

clean_measure = Table(
   'clean_measure', meta,
   Column('station', String, primary_key=True),
   Column('latitude', Float),
   Column('longitude', Float),
   Column('elevation', Float),
   Column('name', String),
   Column('country', String),
   Column('state', String),
)

clean_stations = Table(
   'clean_stations', meta,
   Column('station', String, primary_key=True),
   Column('date', Float),
   Column('precip', Float),
   Column('tobs', Float),
)

meta.create_all(engine)

clean_measure_df = pd.read_csv('clean_measure.csv')
clean_stations_df = pd.read_csv('clean_stations.csv')

Session = sessionmaker(bind=engine)
session = Session()

for _, row in clean_measure_df.iterrows():
    stmt = clean_measure.insert().values(
        station=row['station'],
        latitude=row['latitude'],
        longitude=row['longitude'],
        elevation=row['elevation'],
        name=row['name'],
        country=row['country'],
        state=row['state']
    )
    session.execute(stmt)

for _, row in clean_stations_df.iterrows():
    stmt = clean_stations.insert().values(
        station=row['station'],
        date=row['date'],
        precip=row['precip'],
        tobs=row['tobs']
    )
    session.execute(stmt)

session.commit()

stations_table = Table('clean_stations', meta, autoload=True)
result = session.execute(stations_table.select().limit(5)).fetchall()

for row in result:
    print(row)
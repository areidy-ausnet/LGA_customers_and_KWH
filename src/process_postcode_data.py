from datetime import timedelta
import calendar
import datetime
import os
import geopandas as gpd
from shapely.geometry import Point

import pandas as pd
import structlog
from ausnet_ar_databases import KinetiqStandByDB, Wpdssh02Db,  ImDb, SnetStandByDb
from ausnet_ar_logging import AusnetArLogger
from dateutil.relativedelta import relativedelta
from dateutil.rrule import MONTHLY, rrule

log = structlog.getLogger(__name__)
AusnetArLogger.set_up_logging()

def load_source_sql_queries():
    log.info("loading sql files in sql/kinetiq folder")
    kinetiq_queries = {}
    for file in os.listdir("sql/kinetiq"):
        if file.endswith(".sql"):
            with open("sql/kinetiq/" + file) as f:
                kinetiq_queries[file.replace(".sql", "")] = f.read()
                log.info("loaded sql file: " + file)

    log.info("loading sql files in sql/wpdssh02 folder")
    wpdssh02_queries = {}
    for file in os.listdir("sql/wpdssh02"):
        if file.endswith(".sql"):
            with open("sql/wpdssh02/" + file) as f:
                wpdssh02_queries[file.replace(".sql", "")] = f.read()
                log.info("loaded sql file: " + file)

    log.info("loading sql files in sql/im folder")
    im_queries = {}
    for file in os.listdir("sql/im"):
        if file.endswith(".sql"):
            with open("sql/im/" + file) as f:
                im_queries[file.replace(".sql", "")] = f.read()
                log.info("loaded sql file: " + file)

    log.info("loading sql files in sql/snet folder")
    snet_queries = {}
    for file in os.listdir("sql/snet"):
        if file.endswith(".sql"):
            with open("sql/snet/" + file) as f:
                snet_queries[file.replace(".sql", "")] = f.read()
                log.info("loaded sql file: " + file)

    return kinetiq_queries, wpdssh02_queries, im_queries, snet_queries

def fetch_all_standing_data(from_date, to_date):
    log.info(f"Gathering standing data such as nmi lat/long and nmi_tariff_map ")
    # error check fetch_from_date is less than to_date
    if from_date >= to_date:
        raise ValueError("fetch_from_date must be less than fetch to date")

    # load source sql files from file
    kinetiq_queries, wpdssh02_queries, im_queries, snet_queries = load_source_sql_queries()

    # remove non-standing queries
    del snet_queries["solar_customer_kwh"]
    del kinetiq_queries["nmi_kwh_by_month"]

    # connect to all databases and fetch standing data
    kinetiq_data=connect_to_db_and_fetch_data(KinetiqStandByDB(), kinetiq_queries, from_date, to_date)
    wpdssh02_data=connect_to_db_and_fetch_data(Wpdssh02Db(), wpdssh02_queries, from_date, to_date)
    im_data=connect_to_db_and_fetch_data(ImDb(), im_queries, from_date, to_date)
    snet_data=connect_to_db_and_fetch_data(SnetStandByDb(), snet_queries, from_date, to_date)

    # merge all dictionaries into one
    standing_datasets = {**kinetiq_data, **wpdssh02_data, **im_data, **snet_data}

    # map nmis to tariff and tariff class
    nmi_tariff_map=standing_datasets['nmi_tariff_map']
    tariff_class_map=standing_datasets['tariff_class_map']
    nmi_class_map=pd.merge(nmi_tariff_map, tariff_class_map, on='tariff', how='left')
    # drop tariff column from nmi_class_map
    nmi_class_map=nmi_class_map.drop('tariff', axis=1)

    # merge nmi with their lat long
    nmi_lat_long=standing_datasets['nmi_lat_long']
    nmi_class_map=pd.merge(nmi_class_map, nmi_lat_long, on='nmi', how='left')

    log.info("Finished gathering standing data, exiting")
    return nmi_class_map


def connect_to_db_and_fetch_data(db, queries, from_date, to_date):
    db.connect()
    engine = db.get_engine()

    # initialise dict
    data = {}

    # format dates into yyyy-mm-dd format
    start_date_str = from_date.strftime("%Y-%m-%d")
    end_date_str = to_date.strftime("%Y-%m-%d")

    # loop through the dictionary queries
    for query_name, sql in queries.items():
        log.info(f"Started fetching data for query: {query_name}")
        dataset=pd.read_sql_query( sql.format( start_date_str=start_date_str, end_date_str=end_date_str), engine)
        log.info(f"finished fetching data for query: {query_name}")
        data[query_name] = dataset

    return data

def map_nmis_to_postcodes(nmi_class_map, postcode_shapefile_name):

    # Load the shapefile with postcode boundaries
    postcode_boundaries= None
    for file in os.listdir("../inputs/postcode_shapefiles"):
        if file == postcode_shapefile_name:
            postcode_boundaries = gpd.read_file('../inputs/postcode_shapefiles/'+file)

    # drop unnecessary columns from postcode dataframe
    postcode_boundaries.drop(['POA_NAME21','AUS_CODE21','AUS_NAME21','AREASQKM21','LOCI_URI21','SHAPE_Leng','SHAPE_Area'], axis=1, inplace=True)

    # Create a GeoDataFrame from the nmi coordinate data
    gdf_points = gpd.GeoDataFrame(
        nmi_class_map,
        geometry=gpd.points_from_xy(nmi_class_map.long, nmi_class_map.lat),
        crs='epsg:4326'
    )

    # Drop unnecessary columns
    gdf_points.drop(['lat', 'long'], axis=1, inplace=True)

    # Perform spatial join
    nmi_postcode_map = gpd.sjoin(gdf_points, postcode_boundaries, how="left", predicate="within")

    # drop geocoordinates
    nmi_postcode_map.drop(['geometry','index_right'], axis=1, inplace=True)

    # conver to normal dataframe
    nmi_postcode_map=pd.DataFrame(nmi_postcode_map)

    return nmi_postcode_map


def fetch_all_kwh_data(nmi_class_postcode_map, from_date, to_date):
    # connects to kinetiq and fetches all KWH data for each NMI for a month

    # connect to kinetiq
    conn=KinetiqStandByDB()
    engine=conn.get_engine()

    #get KWh query
    with open("sql/kinetiq/nmi_kwh_by_month.sql") as f:
        nmi_kwh_by_month=f.read()
        log.info("loaded sql file: nmi_kwh_by_month.sql")


    output=[]
    for dt in rrule(MONTHLY, dtstart=from_date, until=to_date):
        start_date = dt.date()
        end_date = start_date + relativedelta(months=1)+relativedelta(days=-1) #last day of month

        # format dates into yyyy-mm-dd format
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        kwh_nmi_month=pd.read_sql_query(nmi_kwh_by_month.format(start_date_str=start_date_str, end_date_str=end_date_str), engine)

        # left join with nmi_class_postcode_map
        kwh_nmi_month=pd.merge(kwh_nmi_month, nmi_class_postcode_map, on='nmi', how='left')

        # drop nmi column
        #kwh_nmi_month=kwh_nmi_month.drop('nmi', axis=1)

        # sum column kwh by "the_month" and "POA_CODE21", also get a count of rows
        kwh_nmi_month=kwh_nmi_month.groupby(['the_month', 'POA_CODE21','class']).agg({'kwh':'sum', 'nmi':'count'}).reset_index()

        # rename columns
        kwh_nmi_month.columns=['the_month', 'POA_CODE21','class', 'kwh', 'nmi_count']

        # append to output
        output.append(kwh_nmi_month)
        log.info(f"Finished fetching kwh data for month: {start_date_str}")

    #concatenate all dataframes
    consumption_kwh_data=pd.concat(output)

    return consumption_kwh_data


def fetch_all_solar_data(nmi_class_postcode_map, from_date, to_date):
    # connects to snet and fetches all solar KWH data for each NMI for a month

    conn=SnetStandByDb()
    engine=conn.get_engine()

    #get solar query
    with open("sql/snet/solar_customer_kwh.sql") as f:
        solar_customer_kwh=f.read()
        log.info("loaded sql file: solar_customer_kwh.sql")

    output = []
    for dt in rrule(MONTHLY, dtstart=from_date, until=to_date):
        start_date = dt.date()
        end_date = start_date + relativedelta(months=1) + relativedelta(days=-1)  # last day of month

        # format dates into yyyy-mm-dd format
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        solar_kwh_nmi_month = pd.read_sql_query(
        solar_customer_kwh.format(start_date_str=start_date_str, end_date_str=end_date_str), engine)

        # left join with nmi_class_postcode_map
        solar_kwh_nmi_month = pd.merge(solar_kwh_nmi_month, nmi_class_postcode_map, on='nmi', how='left')

        # drop nmi column
        #solar_kwh_nmi_month = solar_kwh_nmi_month.drop('nmi', axis=1)

        # sum column kwh by "the_month" and "POA_CODE21", also get a count of rows
        solar_kwh_nmi_month = solar_kwh_nmi_month.groupby(['the_month', 'POA_CODE21']).agg(
            {'solar_kwh_export': 'sum', 'nmi': 'count'}).reset_index()

        # rename columns
        solar_kwh_nmi_month.columns = ['the_month', 'POA_CODE21', 'solar_kwh_export', 'nmi_count']

        # append to output
        output.append(solar_kwh_nmi_month)
        log.info(f"Finished fetching solar data for month: {start_date_str}")

    # concatenate all dataframes
    solar_kwh_data = pd.concat(output)

    return solar_kwh_data

if __name__ == "__main__":
    # set up dates
    from_date = datetime.date(2022, 7, 1)
    to_date = datetime.date(2023, 6, 30)

    # fetch standing data
    nmi_class_map = fetch_all_standing_data(from_date, to_date)

    # map nmis to POA_CODE21
    nmi_class_postcode_map = map_nmis_to_postcodes(nmi_class_map, "POA_2021_AUST_GDA2020.shp")

    # free memory
    del nmi_class_map

    # fetch consumption data for each month
    consumption_kwh_data=fetch_all_kwh_data(nmi_class_postcode_map, from_date, to_date)

    # construct output path
    output_path = os.path.join(os.getcwd(), "..", "outputs")

    # make filename for consumption data
    consumption_filename = f"postcode_consumption_kwh_{from_date.strftime('%Y%m%d')}_{to_date.strftime('%Y%m%d')}.csv"

    # save consumption data to csv
    consumption_kwh_data.to_csv(os.path.join(output_path, consumption_filename), index=False)

    #free memory
    del consumption_kwh_data

    # fetch rooftop solar data for each month
    solar_kwh_data=fetch_all_solar_data(nmi_class_postcode_map, from_date, to_date)

    # make filename for solar data
    solar_filename = f"postcode_solar_kwh_{from_date.strftime('%Y%m%d')}_{to_date.strftime('%Y%m%d')}.csv"
    # save solar data to csv
    solar_kwh_data.to_csv(os.path.join(output_path, solar_filename), index=False)
    log.info("Finished saving data to csv")




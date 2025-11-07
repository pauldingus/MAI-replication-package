##### IMPORTS #####
import pandas as pd
import numpy as np
import gcsfs
import cProfile, traceback
import re, salem, json, warnings, gc
from io import BytesIO
from sqlalchemy import text
from sqlalchemy import create_engine
from google.cloud import storage
client = storage.Client(project='planetupload')
import matplotlib.pyplot as plt
from datetime import datetime
import geopandas as gpd
from IPython.display import display
from sqlalchemy.dialects.mysql import insert
from shapely import buffer
from shapely.geometry import Polygon, MultiPolygon
import statsmodels.api as sm
from scipy import interpolate
from MAI2023.src.dbFunctions import *
import concurrent.futures
from MAI2023.main import *


known_faulty_locs = ['lon36_837lat-1_1718','lon39_2337lat-4_269','lon39_4951lat-3_8969','lon39_523lat-3_543'] # messy shapes
no_perc_export=[]
faulty_prop_export=[]
only_one_shape = ['lon39_4695lat-3_6301','lon39_5301lat-3_9259','lon39_6268lat-3_8199','lon40_0969lat-3_2133','lon40_0974lat-3_2401']# 4 best vars cannot be computed

invalid_market_days = {
    'lon37_6468lat0_0505': 0, # small parking lot outside football stadion
    'lon39_1245lat-4_5529': 3 # noise 
}

freqDayStr_short='w7'
maxRank = 4 # exclude altitude levels above this
ring_area_share = 0.8  # Only consider rings whose area is more than 100(1-X)% of the shape defining the outer border of the ring
_startDateNorm='2021-01-01'
_endDateNorm='2021-12-31'
prefix = "exports-mai2023"
target_folder ='activity_cleaned_2024'
#varsOfInterest=['p50','sumsum', 'ccount']
varsOfInterest=['sumsum', 'ccount']
bucket=client.get_bucket('exports-mai2023')

forMerge=['ident','weekdayThisAreaIsActive','date','mktDay','mktID','locGroup','time','year','month', 'weekday', 'mkt_lat','mkt_lon','time_decimal'] 
patterns_to_drop = ['ground_control','strictnessRank', 'subStrictnessRank''Geography','origName_', 'coorLength_', '.geo', 'system:index_b0', 'system:index', 'weekday_','market']
propToDrop=['quality_category','system:index', '.geo','order_id', 'pixel_resolution','gsd','provider', 'published', 'publishing_stage', 'item_type', 'item_id', 'snow_ice_percent', 'strip_id','updated']

engine = create_engine("mysql+mysqlconnector://root:BMkjM8_)-tN8R33u@34.72.234.161:3306/mai-database", pool_size=10, max_overflow=20)



#---------------------------------------------------#
# Primary Functions
#---------------------------------------------------#

def activityUploader(locs):
    dbColumns = getDBColumns()
    locCount=0
    for loc in locs: #
        print(f'Uploading activity for {loc}...')
        if checkProcessStatus(loc, "activityUpload", setup='activityUpload') == None:
            startProcess(loc, "activityUpload", setup='')
        filePath = f'gs://exports-mai2023/activity_cleaned_2024/df_{loc}.csv'
        locCount = locCount+1
        try:
            raise Exception('Skipping intentionally')
            df = pd.read_csv(filePath)
            market_shapes = gpd.read_file(f"gs://exports-mai2023/{target_folder}/shp_{loc}.shp", driver='ESRI Shapefile')
            if 'PS2.SD' in list(df['instrument']):
                print('Instruments not correctly re-categorized the first time – recreating csv for upload...')
                raise Exception("PS2.SD should not exist in the uploaded CSV")

        except:  
            try: 
                # Get the GEEbucket and locGroup for the current location
                GEEbucket = checkLocationFileStatus(loc, 'bucket')
                locGroup = checkLocationFileStatus(loc, 'locGroup')
                #print(loc, GEEbucket, locGroup)
                # prepare image property dataframe to be merged in later
                df_prop = prepare_properties(locGroup, loc, propToDrop)
                if df_prop.empty:
                    continue

                # Read the activity CSV file
                df = pd.read_csv(f'gs://exports-mai2023/{locGroup}/measures/exportAct5_maxpMax{loc}_{freqDayStr_short}.csv')
                #print('minStrictnessRank',df['strictnessRank'].min())
                # keep only entries that fall between the strictest rank we define and the least strict one for a given shape, but at least 30
                minRank = max(df['strictnessRank'].min(),30)
                df = df[(df['strictnessRank'] <= minRank) & (df['strictnessRank'] >= maxRank)]
                df = df[((df['subStrictnessRank'] <= minRank) & (df['subStrictnessRank'] > maxRank)) | (pd.isna(df['subStrictnessRank'])) | (df['subStrictnessRank'] ==100)]
                #print(df['strictnessRank'].unique().tolist())
                df['subStrictnessRank'] = df['subStrictnessRank'].fillna(100).astype(int)

                eligible_rings = df[df['subStrictnessRank'] != 100].groupby('strictnessRank', as_index=False)['subStrictnessRank'].max()
                additional_rows = pd.DataFrame({
                    'strictnessRank': df['strictnessRank'].unique(),
                    'subStrictnessRank': 100
                })
                eligible_shapes = pd.concat([eligible_rings, additional_rows]).sort_values(by='strictnessRank').reset_index(drop=True)

                df_elig =  pd.merge(df, eligible_shapes, on=['strictnessRank', 'subStrictnessRank'])

                df_elig.rename(columns={'weekdayShp': 'weekdayThisAreaIsActive'}, inplace=True)

                # Extract image id 
                df_elig['ident'] = df_elig['ident'].str.rsplit('_maxpMax', n=1).str[0].str[1:] 
                df_elig['weekdayThisAreaIsActive'] = df_elig['weekdayThisAreaIsActive'].astype(int)
                df_elig['strictnessRank'] = df_elig['strictnessRank'].astype(int)

                # Create area_id column from the strictnessRank variables
                df_elig['strictnessRank_str'] = df_elig['strictnessRank'].apply(prepend_zero_if_single_digit)
                df_elig['subStrictnessRank_str'] = df_elig['subStrictnessRank'].apply(prepend_zero_if_single_digit)
                df_elig['area_id'] = df_elig['strictnessRank_str'].astype(str) + '_' + df_elig['subStrictnessRank_str'].astype(str)

                geos = df_elig['area_id'].unique()

                # Append area id to variable names
                new_column_names = {old_col: old_col + '_maxpMax'  for old_col in varsOfInterest}
                df_elig = df_elig.rename(columns=new_column_names)

                # Assign info variables
                df_elig = infoVars(df_elig, loc, locGroup)

                # Identify market days
                df_elig = identifyMktDays(loc, df_elig, minRank)

                wide_df = df_elig.pivot_table(index=forMerge, columns='area_id', values=list(new_column_names.values()))
                wide_df.columns = ['_'.join(str(s).strip() for s in col if s) for col in wide_df.columns]
                wide_df.reset_index(inplace=True)    

                # Drop unnecessary columns
                wide_df = drop_columns_by_pattern(wide_df, patterns_to_drop)

                # Merge with properties
                wide_df = pd.merge(wide_df, df_prop, on='ident', how='left')

                # Exclude outliers
                wide_df = cleanActMeasures(wide_df, geos, varsOfInterest)
                pd.set_option('display.max_columns', None)

                # Identify varying areas on market days
                df, market_shapes_list = identify_varying_areas(wide_df, locGroup,loc)

                if df.empty:
                    print(f'{loc} has no shapes over which to calculate measures' )
                    df['Location']=loc
                    df = pd.DataFrame(columns=['Location', 'activity_metric'])
                    new_row = {'Location': loc, 'activity_metric': 'no shapes over which to calculate measures'}
                    df.loc[len(df)] = new_row
                    df.to_sql(con=engine, name='activity_market', if_exists='append', index=False, method=insert_on_duplicate, chunksize = 1)
                    continue
                else:
                    market_shapes = gpd.GeoDataFrame(pd.concat(market_shapes_list, ignore_index=True), crs=market_shapes_list[0].crs)
                    
                # Write CSV to GCS
                return_val = upload_df_to_gcs(df, filePath)

            except Exception as e: 
                print(f'Problem with {loc}', e)
                updateProcess(loc, "activityUpload", '', "failed", fail_reason=str(e))
                traceback.print_exc()
                continue

        ### Upload shape and other info
        geoJson = json.loads('{}')
        market_days = []
        for i in range(0,len(market_shapes)):

            gdf_wd = market_shapes.iloc[[i]]
            day = int(gdf_wd.iloc[0]['weekdayShp'])

            #smooth the shape
            shp = salem.transform_geopandas(gdf_wd)
            shp = shp.set_geometry(shp['geometry'].apply(smooth_geometry))
            #shp.boundary.plot()

            #get into json format for the DB
            jsonString = shp.simplify(0.00001).to_json()
            geoJsonDay = json.loads(jsonString)
            geoJsonDay.update({'properties': {'weekday': day}})
            geoJson.update({f'weekday_{day}':geoJsonDay})

            market_days.append(day)

        #upload info data
        jsonString = json.dumps(geoJson)
        mktDayString = ','.join([str(day) for day in market_days])
        lon = market_shapes.iloc[[0]].centroid.get_coordinates()['x'].iloc[0]
        lat = market_shapes.iloc[[0]].centroid.get_coordinates()['y'].iloc[0]
        coordString = '{"lon": ' + str(round(lon,6)) + ', "lat": ' + str(round(lat,6)) + '}'

        cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')
        cursor = cnx.cursor()

        query = f"""
        UPDATE `mai-database`.`location_file` 
        SET market = 1,
            forWebsite = 1,
            marketDays = '{mktDayString}',
            marketLon = {round(lon,6)},
            marketLat = {round(lat,6)},
            mktShape = '{jsonString}',
            marketCentroid = '{coordString}'
        WHERE Location = '{loc}'
        """

        cursor.execute(query)
        cnx.commit()
        cursor.close()
        cnx.close()

        print(f'uploaded shape, weekdays, and coordinates for {loc}')

        ### Clean and upload activity data  
        df = df.drop(columns=df.filter(like='count').columns)

        tokeep=[]
        for market_day in market_days:
            df = df.rename(columns = {f"maxVar_s_{market_day}_maxpMax_1": f"maxVar_s_{market_day}_maxpMax"})
            target_var = df[f"maxVar_s_{market_day}_maxpMax"].unique().tolist()[0].replace("maxpmax", "maxpMax")
            target_var_100 = re.sub(r'_(\d+)$', r'_100', target_var)
            #print(target_var, target_var_100)
            tokeep.extend([df[f"maxVar_s_{market_day}_maxpMax"].unique().tolist()[0].replace("maxpmax", "maxpMax")])
            df.loc[(market_day == df['weekdayThisAreaIsActive']) , 'activity_measure'] = df[target_var_100]

        #normalize the activity measure
        (mean_nonmktday, mean_mktday) = getActivityMeans(df, loc, _startDateNorm, _endDateNorm)
        if mean_nonmktday.empty or mean_mktday.empty:
            print(f"Not enough superdove observations to complete normalization for {loc} -- marking as failed.")
            updateProcess(loc, "activityUpload", '', "failed", fail_reason = "not enough SD observations for normalization")
            continue
        df = pd.merge(df, mean_nonmktday, on=['weekdayThisAreaIsActive', 'instrument'], how='outer', suffixes=('', '_mean_nonmktday'))
        df['activity_measure_mean0'] = df['activity_measure'] - df['activity_measure_mean_nonmktday']
        df = pd.merge(df, mean_mktday, on=['weekdayThisAreaIsActive', 'instrument'], how='outer', suffixes=('', '_mean_mktday'))
        df['activity_measure_norm'] = 100*df['activity_measure_mean0']/df['activity_measure_mean0_mean_mktday'] 
        
        # Apply the function to each string in listA
        tokeep_100 = [replace_after_underscore(col) for col in tokeep]        
        cols_to_drop = [col for col in df.columns if ('maxVar' not in col and 'maxpMax' in col) and col not in tokeep and col not in tokeep_100 ]
        #print(cols_to_drop)
        df = df.drop(columns=cols_to_drop)
        if checkLocationFileStatus(loc, 'stored_in_gcs')==0:
            df = df.drop(columns=['ground_control', 'time', 'locGroup'])
        if checkLocationFileStatus(loc, 'stored_in_gcs') == 1:
            def adjust_ident(ident):
                # Check if the ident string starts with a 6-digit date
                if len(ident.split('_')[0]) < 8:
                    ident = "20" + ident  # Add '20' at the beginning if needed
                return ident

            # Apply adjustments and extract datetime information
            df['acquired'] = df['ident'].apply(lambda x: datetime.strptime(adjust_ident(x)[:15], "%Y%m%d_%H%M%S").strftime("%Y-%m-%dT%H:%M:%S"))
            df['date'] = df['ident'].apply(lambda x: datetime.strptime(adjust_ident(x)[:15], "%Y%m%d_%H%M%S").strftime("%Y-%m-%d"))

        #filter for this market day and clean
        df = df[df['mktDay'] != 99].dropna(subset=['date', 'acquired'])

        df.index = pd.to_datetime(df['acquired'], format='ISO8601')
        df['date'] = pd.to_datetime(df.date)
        df['act_weekly'] = df['activity_measure_norm']
        df['activity_metric'] = pd.NA
        df = df[df['weekdayThisAreaIsActive'].isin(market_days)]

        for market_day in market_days:
            #include target variable for each market day
            df.loc[df['weekdayThisAreaIsActive'] == market_day, 'activity_metric'] = df[f'maxVar_s_{market_day}_maxpMax'].iloc[0]

        df = df.rename(columns={"ident": "image_id"}).filter(dbColumns, axis=1)

        #update df with variables wanted in database
        df['Location'] = loc
        df['act_1monthMA'] = None
        df['act_3monthMA'] = None
        df['acquired'] = pd.to_datetime(df['acquired'], utc=True, errors='coerce')
        toPopulate = ['country', 'admLvl1', 'bucket', 'marketLon', 'marketLat']
        for var in toPopulate: 
            df[var] = checkLocationFileStatus(loc, var)

        #upload to DB
        df.round(5).to_sql(con=engine, name='activity_market', if_exists='append', index=False, method=insert_on_duplicate, chunksize = 1000)
        print(f'uploaded activity for {loc}')
        updateProcess(loc, "activityUpload", '', "complete")
        
def updateActivity(loc, dateID, startDate, endDate):
    logger.debug(f'updating activity for {loc}...')
    startDate = startDate.strftime('%Y-%m-%d')
    endDate = endDate.strftime('%Y-%m-%d')
    startUpdateProcess(loc, 'activityUploadUpdate', '', dateID, startDate, endDate)

    warnings.filterwarnings('ignore')
    pd.set_option('display.width', 100)  # Set display width
    pd.set_option('display.max_columns', 500)  # Show all columns

    locGroup = checkLocationFileStatus(loc, 'locGroup')

    forMerge=['ident','weekdayThisAreaIsActive','date','mktDay','mktID','locGroup','time','year','month', 'weekday', 'mkt_lat','mkt_lon','time_decimal'] 
    patterns_to_drop = ['ground_control','strictnessRank', 'subStrictnessRank''Geography','origName_', 'coorLength_', '.geo', 'system:index_b0', 'system:index', 'weekday_','market']
    propToDrop=['quality_category','system:index', '.geo','order_id', 'pixel_resolution','gsd','provider', 'published', 'publishing_stage', 'item_type', 'item_id', 'snow_ice_percent', 'strip_id','updated']

    freqDayStr_short='w7'
    maxRank = 4 # exclude altitude levels above this
    ring_area_share = 0.8  # Only consider rings whose area is more than 100(1-X)% of the shape defining the outer border of the ring

    prefix = "exports-mai2023"
    target_folder ='activity_cleaned_2024'
    #varsOfInterest=['p50','sumsum', 'ccount']
    varsOfInterest=['sumsum', 'ccount']
    bucket=client.get_bucket('exports-mai2023')

    #get DB columns
    cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')
    cursor = cnx.cursor()
    cursor.execute('SHOW COLUMNS FROM activity_market')
    response = cursor.fetchall()
    dbColumns = [row[0] for row in response]

    cursor.close()
    cnx.close()

    try:
        if not dateID:
            filePath = f'gs://exports-mai2023/activity_cleaned_2024/df_{loc}.csv'
            df = pd.read_csv(filePath)
            market_shapes = gpd.read_file(f"gs://exports-mai2023/{target_folder}/shp_{loc}.shp", driver='ESRI Shapefile')
        else:
            error
    except:  
        try:
            #Get the GEEbucket and locGroup for the current location
            GEEbucket = checkLocationFileStatus(loc, 'bucket')
            locGroup = checkLocationFileStatus(loc, 'locGroup')
            # prepare image property dataframe to be merged in later
            df_prop = prepare_properties(locGroup, loc, propToDrop)

            # Read the activity CSV file
            df = pd.read_csv(f'gs://exports-mai2023/{locGroup}/measures/exportAct5_maxpMax{loc}_{freqDayStr_short}{dateID}.csv')

            # keep only entries that fall between the strictest rank we define and the least strict one for a given shape, but at least 30
            minRank = max(df['strictnessRank'].min(),30)
            df = df[(df['strictnessRank'] <= minRank) & (df['strictnessRank'] >= maxRank)]
            df = df[((df['subStrictnessRank'] <= minRank) & (df['subStrictnessRank'] > maxRank)) | (pd.isna(df['subStrictnessRank'])) | (df['subStrictnessRank'] ==100) ]
            df['subStrictnessRank'] = df['subStrictnessRank'].fillna(100).astype(int)

            eligible_rings = df[df['subStrictnessRank'] != 100].groupby('strictnessRank', as_index=False)['subStrictnessRank'].max()
            additional_rows = pd.DataFrame({
                'strictnessRank': df['strictnessRank'].unique(),
                'subStrictnessRank': 100
            })
            eligible_shapes = pd.concat([eligible_rings, additional_rows]).sort_values(by='strictnessRank').reset_index(drop=True)

            df_elig = pd.merge(df, eligible_shapes, on=['strictnessRank', 'subStrictnessRank'])

            df_elig.rename(columns={'weekdayShp': 'weekdayThisAreaIsActive'}, inplace=True)

            # Extract image id 
            df_elig['ident'] = df_elig['ident'].str.rsplit('_maxpMax', n=1).str[0].str[1:] 
            df_elig['weekdayThisAreaIsActive'] = df_elig['weekdayThisAreaIsActive'].astype(int)
            df_elig['strictnessRank'] = df_elig['strictnessRank'].astype(int)

            # Create area_id column from the strictnessRank variables
            df_elig['strictnessRank_str'] = df_elig['strictnessRank'].apply(prepend_zero_if_single_digit)
            df_elig['subStrictnessRank_str'] = df_elig['subStrictnessRank'].apply(prepend_zero_if_single_digit)
            df_elig['area_id'] = df_elig['strictnessRank_str'].astype(str) + '_' + df_elig['subStrictnessRank_str'].astype(str)

            # Append area id to variable names
            new_column_names = {old_col: old_col + '_maxpMax'  for old_col in varsOfInterest}
            df_elig = df_elig.rename(columns=new_column_names)

            # Assign info variables
            df_elig = infoVars(df_elig, loc, locGroup)

            # Identify market days
            df_elig = identifyMktDays_upd(df_elig,loc)

            #print('loc' ,loc,'ID',dateID)
            wide_df = df_elig.pivot_table(index=forMerge, columns='area_id', values=list(new_column_names.values()))
            wide_df.columns = ['_'.join(str(s).strip() for s in col if s) for col in wide_df.columns]
            wide_df.reset_index(inplace=True)   

            # Drop unnecessary columns
            wide_df = drop_columns_by_pattern(wide_df, patterns_to_drop)

            # Merge with properties
            df_prop['ident_base'] = df_prop['ident'].apply(extract_ident_base)
            wide_df['ident_base'] = wide_df['ident'].apply(extract_ident_base)
            wide_df = pd.merge(wide_df, df_prop, on='ident_base', how='left', suffixes=('', '_prop'))
            wide_df.drop(columns=['ident_base', 'ident_prop'], inplace=True)

            # Exclude outliers
            wide_df = cleanActMeasures_upd(loc, wide_df, varsOfInterest)

            # Identify varying areas on market days
            df, market_shapes_list = identify_varying_areas_upd(wide_df, getMostRecentActivityMetrics(loc), locGroup, loc)
            market_shapes = gpd.GeoDataFrame(pd.concat(market_shapes_list, ignore_index=True), crs=market_shapes_list[0].crs)
        except: 
            print(f'Problem with {loc}')
            if dateID:
                updateUpdateProcess(loc, 'activityUploadUpdate', '', dateID, 'Status', 'failed', replace = True)
                print(f'activity upload failed for {loc}, dateID {dateID}')

    ### Upload shape and other info
    try:  
        ### Clean and upload activity data  
        df = df.drop(columns=df.filter(like='count').columns)

        tokeep=[]
        market_days = market_days_from_location_file(loc)
        for market_day in market_days:
            #print(market_day)
            df = df.rename(columns = {f"maxVar_s_{market_day}_maxpMax_1": f"maxVar_s_{market_day}_maxpMax"})
            target_var = df[f"maxVar_s_{market_day}_maxpMax"].unique().tolist()[0].replace("maxpmax", "maxpMax")
            target_var_100 = re.sub(r'_(\d+)$', r'_100', target_var)
            #print(target_var, target_var_100)
            tokeep.extend([df[f"maxVar_s_{market_day}_maxpMax"].unique().tolist()[0].replace("maxpmax", "maxpMax")])
            df.loc[(market_day == df['weekdayThisAreaIsActive']) , 'activity_measure'] = df[target_var_100]

        #normalize the activity measure
        (mean_nonmktday, mean_mktday) = getActivityMeans_upd(df, loc, _startDateNorm, _endDateNorm)
        df = pd.merge(df, mean_nonmktday, on=[ 'weekdayThisAreaIsActive', 'instrument'], how='outer', suffixes=('', '_mean_nonmktday'))
        df['activity_measure_mean0'] = df['activity_measure'] - df['activity_measure_mean_nonmktday']
        df = pd.merge(df, mean_mktday, on=[ 'weekdayThisAreaIsActive', 'instrument'], how='outer', suffixes=('', '_mean_mktday'))
        df['activity_measure_norm'] = 100*df['activity_measure_mean0']/df['activity_measure_mean0_mean_mktday']

        # Apply the function to each string in listA
        tokeep_100 = [replace_after_underscore(col) for col in tokeep]        
        cols_to_drop = [col for col in df.columns if ('maxVar' not in col and 'maxpMax' in col) and col not in tokeep and col not in tokeep_100 ]
        #print(cols_to_drop)
        df = df.drop(columns=cols_to_drop).dropna(subset = ['ident'])
        if checkLocationFileStatus(loc, 'stored_in_gcs')==0:
            df = df.drop(columns=['ground_control', 'time', 'locGroup'])
        if checkLocationFileStatus(loc, 'stored_in_gcs') == 1:
            def adjust_ident(ident):
                # Check if the ident string starts with a 6-digit date
                if len(ident.split('_')[0]) < 8:
                    ident = "20" + ident  # Add '20' at the beginning if needed
                return ident

            # Apply adjustments and extract datetime information
            df['acquired'] = df['ident'].apply(lambda x: datetime.strptime(adjust_ident(x)[:15], "%Y%m%d_%H%M%S").strftime("%Y-%m-%dT%H:%M:%S"))
            df['date'] = df['ident'].apply(lambda x: datetime.strptime(adjust_ident(x)[:15], "%Y%m%d_%H%M%S").strftime("%Y-%m-%d"))

        #filter for this market day and clean
        df = df[df['mktDay'] != 99]#.dropna(subset=['date', 'acquired'])
        df.index = pd.to_datetime(df.acquired)
        df['date'] = pd.to_datetime(df.date)
        df['act_weekly'] = df['activity_measure_norm']
        df['activity_metric'] = pd.NA
        df = df[df['weekdayThisAreaIsActive'].isin(market_days)]

        #update activity_metric column
        activity_metrics = getMostRecentActivityMetrics(loc)
        for market_day in market_days:
            #include target variable for each market day
            df.loc[df['weekdayThisAreaIsActive'] == market_day, 'activity_metric'] = activity_metrics[market_day]

        #clean and update df with variables wanted in database
        df = df.dropna(subset = 'ident').rename(columns={"ident": "image_id"}).filter(dbColumns, axis=1)
        df['act_1monthMA'] = None
        df['act_3monthMA'] = None
        df['Location'] = loc
        df['acquired'] = pd.to_datetime(df['acquired'])
        toPopulate = ['country', 'admLvl1', 'bucket', 'marketLon', 'marketLat']
        for var in toPopulate: 
            df[var] = checkLocationFileStatus(loc, var)

        #upload to DB
        engine = create_engine("mysql+mysqlconnector://root:BMkjM8_)-tN8R33u@34.72.234.161:3306/mai-database")
        df.round(6).to_sql(con=engine, name='activity_market', if_exists='append', index=False, method=insert_on_duplicate)

        if dateID:

            #confirm success and update location file
            if list(df['image_id'])[-1] in list(pd.read_sql(f'SELECT image_id FROM activity_market WHERE Location = "{loc}" LIMIT 10000', engine)['image_id']):
                endDate = checkUpdateProcessStatus(loc, 'activityExportUpdate', 'exportAct5', dateID, column = 'updateEndDate')
                updateLocationFileStatus(loc, 'lastActivityUpdate', endDate, replace = True)
                updateUpdateProcess(loc, 'activityUploadUpdate', '', dateID, 'Status', 'complete', replace = True)
                print(f'Successfully updated activity for {loc} -- start: {startDate}, end: {endDate.date()}, observations: {df.shape[0]}.')

    except Exception as e: 
        print(f"upload problem with {loc}:{e}")
        if "['activity_measure'] not in index" in f"{e}" :
            updateUpdateProcess(loc, 'activityUploadUpdate', '', dateID, 'Status', 'failed', replace = True)
            print(f'recorded activityUploadUpdate for {loc} as failed.')
        else:
            updateUpdateProcess(loc, 'activityUploadUpdate', '', dateID, 'Status', 'failed', replace = True)
            updateUpdateProcess(loc, 'activityUploadUpdate', '', dateID, 'FailReason', str(e), replace = True)
            print(f'recorded activityUploadUpdate for {loc} as failed.')

    clear_local_dataframes(locals())
        
#---------------------------------------------------#
# Update Functions
#---------------------------------------------------#

def getDBColumns():
    cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')
    cursor = cnx.cursor()

    cursor.execute('SHOW COLUMNS FROM activity_market')
    response = cursor.fetchall()
    dbColumns = [row[0] for row in response]

    cursor.close()
    cnx.close()
    
    return dbColumns

def update_old_norm_values(loc, mean_mktday, mean_nonmktday):
    means = pd.merge(mean_nonmktday, mean_mktday, on = ['weekdayThisAreaIsActive', 'instrument'])
    means.rename(columns = {'activity_measure_mean0':'activity_measure_mean0_mean_mktday', 
                            'activity_measure':'activity_measure_mean_nonmktday'}, inplace = True)
    for index, row in means.iterrows():
        with engine.begin() as conn:
            # Set the two baseline values
            conn.execute(text("""
                UPDATE activity_market
                SET activity_measure_mean_nonmktday = :mean_nonmktday,
                    activity_measure_mean0_mean_mktday = :mean0_mktday
                WHERE Location = :loc
                AND instrument = :instrument
                AND weekdayThisAreaIsActive = :weekdayThisAreaIsActive
            """), {
                'mean_nonmktday': row['activity_measure_mean_nonmktday'],
                'mean0_mktday': row['activity_measure_mean0_mean_mktday'],
                'loc': loc,
                'instrument': row['instrument'],
                'weekdayThisAreaIsActive': row['weekdayThisAreaIsActive']
            })

            # Recalculate activity_measure_mean0
            conn.execute(text("""
                UPDATE activity_market
                SET activity_measure_mean0 = activity_measure - activity_measure_mean_nonmktday
                WHERE Location = :loc
                AND instrument = :instrument
                AND weekdayThisAreaIsActive = :weekdayThisAreaIsActive
            """), {
                'loc': loc,
                'instrument': row['instrument'],
                'weekdayThisAreaIsActive': row['weekdayThisAreaIsActive']
            })

            # Normalize
            conn.execute(text("""
                UPDATE activity_market
                SET activity_measure_norm = 100 * (activity_measure_mean0 / activity_measure_mean0_mean_mktday)
                WHERE Location = :loc 
                AND activity_measure_mean0_mean_mktday IS NOT NULL AND activity_measure_mean0_mean_mktday != 0
                AND instrument = :instrument
                AND weekdayThisAreaIsActive = :weekdayThisAreaIsActive
            """), {
                'loc': loc,
                'instrument': row['instrument'],
                'weekdayThisAreaIsActive': row['weekdayThisAreaIsActive']
            })

def infoVars(df, mktID, locGroup): # assign info variables based on date and location
    df['mktID'] = mktID
    df['locGroup'] = locGroup
    country = checkLocationFileStatus(mktID, 'country')
    df['country'] = country
    try: # Necessary because some exports have band names starting with 1_ or 2_, not the date. Comes from merge of two image collections ic_old and ic_new
        df['date'] = pd.to_datetime(df['ident'].apply(lambda x: datetime.strptime(x[:8], "%Y%m%d").date()))
        df['time'] = df['ident'].apply(lambda x: datetime.strptime(x[9:15], "%H%M%S").time())
    except:         
        try:
            df['date'] = pd.to_datetime(df['ident'].apply(lambda x: datetime.strptime(x[2:10], "%Y%m%d").date()))
            df['time'] = df['ident'].apply(lambda x: datetime.strptime(x[11:17], "%H%M%S").time())
        except:
            try:         
                df['date'] = pd.to_datetime(df['ident'].apply(lambda x: datetime.strptime(x[0:6], "%y%m%d").date()))
                df['time'] = df['ident'].apply(lambda x: datetime.strptime(x[7:13], "%H%M%S").time())
            except:
                try:
                    df['date'] = pd.to_datetime(df['ident'].apply(lambda x: datetime.strptime(x[2:8], "%y%m%d").date()))
                    df['time'] = df['ident'].apply(lambda x: datetime.strptime(x[8:14], "%H%M%S").time())
                except Exception as e:
                    print("not a valid date format", e)
                    pass
        
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['time_decimal'] = df['time'].apply(lambda t: t.hour + t.minute / 60 + t.second / 3600)
    df['weekday'] = (df['date'].dt.weekday + 1) % 7
    df['mkt_lat'] = pd.to_numeric(df['mktID'].str.extract(r'lon(-?\d+)_(\d+)').apply(lambda x: f"{x[0]}.{x[1]}", axis=1))
    df['mkt_lon'] = pd.to_numeric(df['mktID'].str.extract(r'lat(-?\d+)_(\d+)').apply(lambda x: f"{x[0]}.{x[1]}", axis=1))
    if country=="Kenya": # For some locations in Kenya, the lon and lat coordinates were flipped in their mktid
        df['origLat'] = df['mkt_lat']
        df.loc[df['mkt_lat'] > 30, 'mkt_lat'] = df['mkt_lon']
        df.loc[df['mkt_lon'] < 30, 'mkt_lon'] = df['origLat']
        df.drop(columns=['origLat'], inplace=True)
    if country=="Ethiopia": # For some locations in Ethiopia, the lon and lat coordinates were flipped in their mktid
        df['origLat'] = df['mkt_lat']
        df.loc[df['mkt_lat'] > 20, 'mkt_lat'] = df['mkt_lon']
        df.loc[df['mkt_lon'] < 20, 'mkt_lon'] = df['origLat']
        df.drop(columns=['origLat'], inplace=True)
    return df

def interval_mean(df, col, lower_q=0.10, upper_q=0.90):
    lower = df[col].quantile(lower_q)
    upper = df[col].quantile(upper_q)
    result_df = df[["weekdayThisAreaIsActive", "instrument"]].drop_duplicates()
    result_df[col] = df[(df[col] >= lower) & (df[col] <= upper)][col].mean()
    return result_df

def apply_smooth(df, y_col="activity_measure", startDate=None, endDate=None, show_plot=False):

    x_col = "date_diff" # variable to smooth over
    loc = df.iloc[0]['mktID']
    
#     print('start date: ', startDate)
#     print('end date: ', endDate)
    
    # Drop superdove observations before March 2021 (https://docs.planet.com/data/imagery/planetscope/)
    if '.SD' in df.iloc[0]['instrument']:
        df = df[df['date'] >= '2020-03-01']

    # Drop NA and consolidate to 1 observation per date (necessary for smoothing)
    df_notna = df.dropna(subset=[y_col]).groupby('date').agg({y_col:'mean'}).reset_index()

    # Buffer the date range to make the smoothing more stable over the range of interest
    if startDate and endDate:
        df_notna = df_notna.loc[(df_notna["date"] >= (startDate - pd.Timedelta(days=182))) & 
                                (df_notna["date"] <= (endDate   + pd.Timedelta(days=182)))]

    # Sort by date and create an integer value representing date (date_diff)
    df_notna = df_notna.sort_values(by="date").reset_index(drop=True)
    df_notna["date_diff"] = (df_notna["date"] - pd.to_datetime("2000-01-01")).dt.days

    # If the data is too small to generate a spline, use simple interpolation and calculate the mean
    if df_notna.shape[0] < 10:

        # Set the index to be the date (for interpolation)
        df_notna.index = df_notna["date"]  

        # If we have start and end dates, extend out to those
        if startDate and endDate: 
            new_index = pd.date_range(startDate, endDate, freq="D")
            df_notna = df_notna.reindex(new_index)

        # Interpolate and get the mean
        mean_value = df_notna[y_col].interpolate("time").mean()
        result_df = df[["weekdayThisAreaIsActive", "instrument"]].drop_duplicates()
        result_df[y_col] = mean_value
        return result_df

    x_vals = np.array(df_notna[x_col]).astype(np.int64)  # all x values for smoothing
    y_vals = np.array(df_notna[y_col])  # all y values for smoothing

    # Create an array over the whole range of x-values
    x_smooth = np.linspace(x_vals.min(), x_vals.max(), x_vals.max() - x_vals.min() + 1).astype(np.int64)  

    # Parameterize the smoother and create a smoothed output
    spl = interpolate.UnivariateSpline(x=x_vals, y=y_vals, s=len(y_vals) * np.var(y_vals) / 1.5)
    y_smooth = spl(x_smooth)  # create an array of smoothed values at all values of x
    
    # Clip the smoothed values to the original range
    y_smooth = np.clip(y_smooth, np.min(y_vals), a_max=np.max(y_vals))

    # Calculate the mean only over the smoothed values in the original date range
    if startDate and endDate:
        
        # Store simple mean for later sanity check
        mean_value_simple = df_notna[(df_notna['date'] >= startDate) & (df_notna['date'] <= endDate)][y_col].mean()
        sd = df_notna[(df_notna['date'] >= startDate) & (df_notna['date'] <= endDate)][y_col].std()
        
        # Calculate smoothed mean over buffered range
        mean_range = (x_smooth >= (startDate - pd.to_datetime("2000-01-01")).days) & (x_smooth <= (endDate - pd.to_datetime("2000-01-01")).days)
        mean_value = y_smooth[mean_range].mean()

    # If no start dates given, calculate within a buffered region
    else:
        
        # Store simple mean for later sanity check
        mean_value_simple = df_notna[y_col].mean()
        sd = df_notna[y_col].std()
        
        # Calculate smoothed mean over buffered range
        mean_range = (x_smooth >= x_smooth.min() + 182) & (x_smooth <= x_smooth.max() - 182)
        mean_value = y_smooth[mean_range].mean()

        
#     print('simple mean: ', mean_value_simple)
#     print('smoothed mean: ', mean_value)
#     print('sd: ', sd)
    
    # Mark if there is a concerning difference between simple and smoothed means
    if abs(mean_value-mean_value_simple) > sd:
        print(f'Warning: difference between smoothed mean and simple mean for {loc} exceeds the standard deviation. Updating process notes.')
        updateProcess(loc, 'activityUpload', '', 'Large means difference in activity upload', column = 'notes')

    if show_plot:
        plt.scatter(df_notna[x_col], df_notna[y_col])
        plt.plot(x_smooth, y_smooth, color='red', label='Gauss Smoothed Total Range')
        plt.plot(x_smooth[mean_range], y_smooth[mean_range], color='blue', label='Gauss Smoothed Mean Range')
        plt.axhline(y=mean_value, color='r', linestyle='--', label=f'Horizontal line at y={round(mean_value, 4)}')
        plt.legend()
        plt.tight_layout()
        plt.show()

    result_df = df[["weekdayThisAreaIsActive", "instrument"]].drop_duplicates()
    result_df[y_col] = mean_value

    return result_df

def getActivityMeans(df, loc, startDate, endDate):
    
    #download all previous activity for this loc
    #dbActivity = pd.read_sql(f'SELECT image_id, instrument, weekdayThisAreaIsActive, mktDay, activity_measure, date FROM activity_market WHERE Location = "{loc}" LIMIT 1000000', engine).rename(columns = {'image_id':'ident'})
    
    #convert dates to datetime format
    startDate = pd.to_datetime(startDate)
    endDate = pd.to_datetime(endDate)    
    #cols to merge
    cols = ['mktID', 'instrument', 'ident', 'weekdayThisAreaIsActive', 'mktDay', 'activity_measure', 'date']
    allActivity=df[cols]
    #combine activity from the database with new activity and delete duplicate and unnecessary rows
    #allActivity = pd.concat([dbActivity[cols], df[cols]], axis = 0)
    allActivity.date = pd.to_datetime(allActivity['date']).dt.date
    allActivity = allActivity.drop_duplicates(subset = ['instrument', 'weekdayThisAreaIsActive', 'ident']).dropna(subset = 'activity_measure')
    allActivity = allActivity[allActivity['instrument'] != 'PS2'].dropna(subset=['activity_measure'])
    allActivity['mktDay'] = allActivity['mktDay'].astype(int)

    allActivity['date'] = pd.to_datetime(allActivity['date'])
    allActivity = allActivity.sort_values(by='date')
    
    # Apply  smoothing for each group separately
#     print(f'getting mean_nonmktday, initial upload for {loc}')
    mean_nonmktday = allActivity[allActivity['mktDay']==0].groupby(['weekdayThisAreaIsActive', 'instrument'], group_keys=False).apply(lambda g: interval_mean(g, 'activity_measure'))
    allActivity = pd.merge(allActivity, mean_nonmktday, on=['weekdayThisAreaIsActive', 'instrument'], how='outer', suffixes=('', '_mean_nonmktday'))
    
#     print(f'getting activity_measure_mean0_mean_mktday, initial upload for {loc}')
    # Zero the market data by subtracting nonmarket average, calculate mean within reference range
    allActivity['activity_measure_mean0'] = allActivity['activity_measure'] - allActivity['activity_measure_mean_nonmktday']
    mean_mktday = allActivity[allActivity['mktDay']==1].groupby(['weekdayThisAreaIsActive', 'instrument'], group_keys=False).apply(lambda g: apply_smooth(g,'activity_measure_mean0', startDate, endDate))
    
    return mean_nonmktday, mean_mktday

def identifyMktDays(loc, df, minRank): # identify market days based on detected areas and their threshold values
    
    # List all maximum threshold values on the days-of-week where we detected something and that detection falls below a threshold 
    min_thres_by_day = df.groupby('weekdayThisAreaIsActive')['strictnessRank'].min()
    if loc in invalid_market_days: # remove  known misdetections from visual checks of outputs
        value_to_remove = invalid_market_days[loc]
        if value_to_remove in min_thres_by_day:
            del min_thres_by_day[value_to_remove]
    #print('strictness rank and active weekdays',min_thres_by_day)
    # Find the clearest detection 
    lowest_thres = min_thres_by_day.min()
    #print('lowest strictness rank',lowest_thres)
    # Filter unique days of week where the threshold is within 3 ranks of the lowest threshold value -> identifies all similarly high detections
    localMktDays = list(min_thres_by_day[min_thres_by_day - lowest_thres <= 3].index.unique())
    def find_position(weekday):
        try:
            return list(localMktDays).index(weekday)
        except ValueError:
            return -1  # Return 0 if the weekday is not found in the list
    df['pos'] = df['weekday'].apply(find_position)
    df['mktDay'] = None
    df.loc[(df['weekday'] == df['weekdayThisAreaIsActive']) & (df['pos'] >= 0), 'mktDay'] = 1 # detected market day
    df.loc[ (df['pos'] == -1), 'mktDay'] = 0 # detected non-market day
    df.loc[(df['weekday'] != df['weekdayThisAreaIsActive']) & (df['pos'] >= 0), 'mktDay'] = 99 # observation of detected market area for a given weekday on a different weekday
    return df

def cleanActMeasures(df, geos, varsOfInterest): 
    # Set values to NA that exceed the median value per market, weekday of operation
    # and instrument by more than twice the IQR , calculated over the period 
    # outside Covid and for typical times and good images

    df['median_time'] = df.groupby('instrument')['time_decimal'].transform('median')
    df['diff_to_median_time'] = abs(df['time_decimal'] - df['median_time'])
    mask = (
        (df['date'].between('2020-03-01', '2021-02-28')) | # potentially covid-affected
        (df['date'] < '2018-01-01') |                      # generally noisier because of sparse imagery
        (df['diff_to_median_time'] > .5) |                  # differing sun angle
        ((df['clear_percent'].notnull()) & (df['clear_percent'] < 10)) | # noisy imagery
        ((df['cloud_percent'].notnull()) & (df['cloud_percent'] > 50))
    )
    # Create a new column 'exclDates' based on the mask
    df['exclDates'] = mask.astype(int)
    for b in geos: # within each possible area
        df[f'sumsum_maxpMax_{b}'] = df[f'sumsum_maxpMax_{b}'] / df[f'ccount_maxpMax_{b}'] # convert sum variable into mean deviations

        # Typical number of pixels per shape
        max_count = df.loc[df['exclDates'] != 1].groupby(['weekdayThisAreaIsActive', 'mktDay'])[f'ccount_maxpMax_{b}'].max().reset_index()
        df = pd.merge(df, max_count, on=[ 'weekdayThisAreaIsActive', 'mktDay'], how='outer', suffixes=('', '_max_count'))        

        for p in varsOfInterest:
            try:
                # set to NA those values coming from images that cover less than 50% of the typical footprint
                df.loc[df[f'ccount_maxpMax_{b}']  < 0.5 *(df[f'ccount_maxpMax_{b}_max_count']), f'{p}_maxpMax_{b}'] = pd.NA

                # calculate median, iqr by detected area and sensor, and merge to dataframe
                median = df.loc[df['exclDates'] != 1].groupby(['weekdayThisAreaIsActive', 'mktDay', 'instrument'])[f'{p}_maxpMax_{b}'].quantile(0.5).reset_index()
                df = pd.merge(df, median, on=[ 'weekdayThisAreaIsActive', 'mktDay', 'instrument'], how='outer', suffixes=('', '_median'))

                p25 = df.loc[df['exclDates'] != 1].groupby(['weekdayThisAreaIsActive', 'mktDay', 'instrument'])[f'{p}_maxpMax_{b}'].quantile(0.25)
                p75 = df.loc[df['exclDates'] != 1].groupby(['weekdayThisAreaIsActive', 'mktDay', 'instrument'])[f'{p}_maxpMax_{b}'].quantile(0.75)
                iqr = (p75-p25).reset_index()
                df = pd.merge(df, iqr, on=[ 'weekdayThisAreaIsActive', 'mktDay', 'instrument'], how='outer', suffixes=('', '_iqr'))
                
                # set to NA those values that are more than twice the IQR above the median
                df.loc[df[f'{p}_maxpMax_{b}']  > (df[f'{p}_maxpMax_{b}_median'] + 2 * df[f'{p}_maxpMax_{b}_iqr']), f'{p}_maxpMax_{b}'] = pd.NA
                df = df.drop([f'{p}_maxpMax_{b}_median', f'{p}_maxpMax_{b}_iqr'], axis=1)    

            except Exception as e:
                print('Error in cleanActMeasures', e)
                pass
    return df

def insert_on_duplicate(table, conn, keys, data_iter):
    insert_stmt = insert(table.table).values(list(data_iter))
    on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(insert_stmt.inserted)
    conn.execute(on_duplicate_key_stmt)

def contains_substring(s, substrings):
    for substring in substrings:
        if substring in s:
            return True
    return False

def drop_columns_by_pattern(df, patterns_to_drop):
    for pattern in patterns_to_drop:
        try:
            df = df.drop(df.filter(like=pattern).columns, axis=1)
        except Exception as e:
            print(f"Error occurred while dropping columns for pattern '{pattern}': {e}")
    return df

def determine_sensor(row):
    image_id = row['ident']
    condition1 = '3B' in image_id[-2:]
    condition2 = '_1_' in image_id
    if condition1 or condition2:
        return 'PS2'
    else:
        return 'PSB.SD'
    
def getWeekDayStr(weekdayNum):
    lookup = {0: 'SUN', 1: 'MON', 2: 'TUE', 3: 'WED', 4: 'THU', 5: 'FRI', 6: 'SAT'}
    return lookup[weekdayNum]
    
def prepare_properties(locGroup, loc, propToDrop):  
    
    if checkLocationFileStatus(loc, 'stored_in_gcs')==0:
        try:
            prop_path = f'gs://exports-mai2023/{locGroup}/properties/propEx_{locGroup}_{loc}.csv'
            df_prop = pd.read_csv(prop_path)
        except FileNotFoundError:
            print(f'Properties csv missing for {loc} at {prop_path}: deleting process status to re-initiate prop export.')
            updateProcess(loc, "PropExport", "", "incomplete")
            return pd.DataFrame()
        # Extract 'ident' from 'system:index' column
        df_prop['ident'] = df_prop['system:index'].str.slice(stop=23) 
        # Determine the imagery generation of each image
        df_prop['instrument'] = df_prop.apply(determine_sensor, axis=1)

        
    if checkLocationFileStatus(loc, 'stored_in_gcs')==1:
        try:
            prop_path = f"gs://ps-imgs-mai1/{locGroup}/properties/propEx_{locGroup}_{loc}.csv"
            df_prop = pd.read_csv(prop_path)
        except FileNotFoundError:
            print(f'Properties csv missing for {loc} at {prop_path}: deleting process status to re-initiate prop export.')
            updateProcess(loc, "PropExport", "", "incomplete")
            return pd.DataFrame()
        df_prop['ident'] = df_prop['id']
        df_prop['id']=np.nan
    
    
    # Drop specified properties from the DataFrame
    for prop in propToDrop:
        try:
            df_prop = df_prop.drop(prop, axis=1)
        except KeyError:
            pass
    return df_prop  

def identify_varying_areas(wide_df, locGroup,loc): # Identify the largest ring in which P75 non-market day activity still does not exceed P50 market day activity
    market_days = wide_df.loc[wide_df['mktDay'] == 1, 'weekday'].unique().tolist()
    valid = False
    #print('market_days_', market_days)
    gdfs = [] # dataframe to hold the selected shapes
    for market_day in market_days:
        #print('market_days', market_days, market_day)
        df_mktDays = wide_df[(wide_df['mktDay'] == 1) 
                     & (wide_df['exclDates'] == 0) 
                     & (wide_df['clear_percent'] > 90) 
                     & (wide_df['weekdayThisAreaIsActive']==market_day) 
                     & (wide_df['weekday']==market_day) 
                     & (wide_df['diff_to_median_time'] <.5)]

        filtered_columns_sum = df_mktDays.loc[:, df_mktDays.columns.str.contains('sumsum') & 
                                     ~df_mktDays.columns.str.contains('_100')]

        # Exclude columns that are all NA
        filtered_columns_sum = filtered_columns_sum.loc[:, filtered_columns_sum.notna().any()].columns.tolist()
        if filtered_columns_sum:
            valid = True
            
            #print(filtered_columns_sum)
            df_nonmktDays = wide_df[(wide_df['mktDay'] == 0) 
                                    & (wide_df['exclDates'] == 0) 
                                    & (wide_df['clear_percent'] > 90)
                                    & (wide_df['diff_to_median_time'] <.5) 
                                    & (wide_df['weekdayThisAreaIsActive']==market_day)]

            p75_nonmktDays_sum = df_nonmktDays[filtered_columns_sum].dropna(subset=filtered_columns_sum, how='all').quantile(0.75)    
            # keep high quality images, separately for market and non-market days

            # Calculate variance and mean for percentiles (filtered_columns_p)
            p50_mktDays_sum = df_mktDays[filtered_columns_sum].dropna(subset=filtered_columns_sum, how='all').quantile(0.5)
            result = pd.concat([p50_mktDays_sum, p75_nonmktDays_sum], axis=1)
            result.columns = ['p50_mktDays_sum', 'p75_nonmktDays_sum']

            first_row_index = (result['p75_nonmktDays_sum'] > result['p50_mktDays_sum']).replace(False, pd.NA).idxmax()
            if pd.isna(first_row_index):
                first_row_index= result.iloc[-1].name

            #print("First row where p75_nonmktDays_sum > p50_mktDays_sum: ",first_row_index)

            # Update DataFrame with name of area per weekday that we consider the market area
            wide_df[f'maxVar_s_{market_day}_maxpMax'] = first_row_index
            #print(loc,first_row_index)
            filtered_gdf = select_areas(market_day, first_row_index, locGroup,loc)
            gdfs.append(filtered_gdf)
        else:
            continue
            
    if valid == True:
        return wide_df, gdfs
    else:
        return pd.DataFrame(),pd.DataFrame()

def select_areas(market_day, first_row_index, locGroup, loc): #select the shapes associated with the selected market area
    # extract substring between second last and last instance of _
    temp = first_row_index.split('_')
    if len(temp) >= 2:
        minRing =  int(temp[-2])
    else:
        minRing = None  # Return None if there aren't enough parts
    logger.debug('minRing', minRing)
    # load shapefile
    #print(locGroup, loc)
    shp_path = f'gs://exports-mai2023/{locGroup}/shapes/shp_MpM6_{locGroup}{loc}.shp'
    gdf = gpd.read_file(shp_path)    
    filtered_gdf = gdf[(gdf['weekdayShp'] == market_day) & 
                   (gdf['strictness'] == minRing) & 
                   (gdf['subStrictn'] == 100)].copy()
    #filtered_gdf.plot()
    filtered_gdf.loc[:, 'mktid'] = loc  # Use .loc to set values
    return filtered_gdf

def prepend_zero_if_single_digit(value):
    if len(str(value)) == 1:
        return '0' + str(value)
    else:
        return str(value)

def replace_after_underscore(s):
    return s[:s.rfind('_') + 1] + '100'

    
def check_file_exists(bucket, file_path):
    """Check if a file exists in a Google Cloud Storage bucket."""
    blob = bucket.blob(file_path)
    return blob.exists()

def smooth_geometry(geo):
    try:
        smoothed_polygons = []
        for poly in geo.geoms:
            smoothed_polygon = buffer(poly, distance=0.0001, quad_segs=2)
            smoothed_polygon = buffer(smoothed_polygon, distance=-0.0001, quad_segs=2)
            if isinstance(smoothed_polygon, Polygon):
                smoothed_polygons.append(smoothed_polygon)
            elif isinstance(smoothed_polygon, MultiPolygon):
                smoothed_polygons.extend(smoothed_polygon.geoms)
        return MultiPolygon(smoothed_polygons)
    except:
        smoothed_polygon = buffer(geo, distance=0.0001, quad_segs=2)
        smoothed_polygon = buffer(smoothed_polygon, distance=-0.0001, quad_segs=2)
        if isinstance(smoothed_polygon, Polygon):
            return Polygon(smoothed_polygon)
        elif isinstance(smoothed_polygon, MultiPolygon):
            print("Result is a MultiPolygon. Using the largest polygon.")
            # Select the largest polygon by area
            return Polygon(max(smoothed_polygon.geoms, key=lambda p: p.area))
        else:
            raise ValueError(f"Unexpected geometry type after smoothing: {type(smoothed_polygon)}")  
            
def getActivityMeans_upd(df, loc, startDate, endDate):
    #convert dates to datetime format
    startDate = pd.to_datetime(startDate)
    endDate = pd.to_datetime(endDate)    
    
    minDateDF = df['date'].min()
    minDateStr = minDateDF.strftime('%Y-%m-%d')
    #print('minDateStr',minDateStr, minDateDF)
    #download all previous activity for this loc
    #engine = create_engine("mysql+mysqlconnector://root:BMkjM8_)-tN8R33u@34.72.234.161:3306/mai-database")
    dbActivity = pd.read_sql(f'SELECT image_id, Location, instrument, weekdayThisAreaIsActive, mktDay, activity_measure, date FROM activity_market WHERE Location = "{loc}" AND date<"{minDateDF}" LIMIT 10000000', engine).rename(columns = {'image_id':'ident'})
    dbActivity.rename(columns = {'Location': 'mktID'}, inplace=True)
    #cols to merge
    cols = ['mktID', 'instrument', 'ident', 'weekdayThisAreaIsActive', 'mktDay', 'activity_measure', 'date']

    #combine activity from the database with new activity and delete duplicate and unnecessary rows
    allActivity = pd.concat([dbActivity[cols], df[cols]], axis = 0)
    allActivity.date = pd.to_datetime(allActivity['date']).dt.date
    allActivity = allActivity.drop_duplicates(subset = ['instrument', 'weekdayThisAreaIsActive', 'ident']).dropna(subset = 'activity_measure')
    allActivity = allActivity[allActivity['instrument'] != 'PS2'].dropna(subset=['activity_measure'])
    allActivity['mktDay'] = allActivity['mktDay'].astype(int)

    allActivity['date'] = pd.to_datetime(allActivity['date'])
    allActivity = allActivity.sort_values(by='date')
    
    # Use interval mean to get the non-market-day mean
#     print(f'getting mean_nonmktday, initial upload for {loc}')
    mean_nonmktday = allActivity[allActivity['mktDay']==0].groupby(['weekdayThisAreaIsActive', 'instrument'], group_keys=False).apply(lambda g: interval_mean(g, 'activity_measure'))
    allActivity = pd.merge(allActivity, mean_nonmktday, on=['weekdayThisAreaIsActive', 'instrument'], how='outer', suffixes=('', '_mean_nonmktday'))
    
#     print(f'getting activity_measure_mean0_mean_mktday, initial upload for {loc}')
    #zero the market data by subtracting nonmarket average, calculate mean within reference range
    allActivity['activity_measure_mean0'] = allActivity['activity_measure'] - allActivity['activity_measure_mean_nonmktday']

    mean_mktday = allActivity[allActivity['mktDay']==1].groupby(['weekdayThisAreaIsActive', 'instrument'], group_keys=False).apply(lambda g: apply_smooth(g,'activity_measure_mean0',startDate, endDate))
    
    update_old_norm_values(loc, mean_mktday, mean_nonmktday)
    
    return mean_nonmktday, mean_mktday

def identifyMktDays_upd(df, loc): # identify market days based on detected areas and their threshold values
    toMerge = getMarketDays(loc)
    df_merged = pd.merge(df, toMerge, on=['weekday','weekdayThisAreaIsActive'], how='outer')
    return df_merged

def getMarketDays(loc):
    
    cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')
    cursor = cnx.cursor()

    query = f'''
            SELECT weekday, weekdayThisAreaIsActive, mktDay FROM `mai-database`.activity_market 
            WHERE Location = '{loc}' 
            ORDER BY acquired DESC
            LIMIT 1000000
            '''
    
    cursor.execute(query)
    response = cursor.fetchall()
    cursor.close()
    cnx.close()

    df = pd.DataFrame(list(set(response)), columns=['weekday', 'weekdayThisAreaIsActive', 'mktDay']).sort_values(by='weekday')
    df = df.astype(float).astype(int)
    #display(df)
    marketDayList = df.loc[df['mktDay'] == 1, 'weekday'].tolist()

    new_rows = []
    for value in df['weekdayThisAreaIsActive'].unique():
        for item in marketDayList:
            if value !=item:
                new_rows.append({'weekday': value, 'weekdayThisAreaIsActive': item, 'mktDay': 99})

    df_extended = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    return df_extended

def getMostRecentActivityMetrics(loc):
    
    cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')
    cursor = cnx.cursor()

    query = f'''
            SELECT weekday, activity_metric FROM `mai-database`.activity_market 
            WHERE Location = '{loc}' 
            AND mktDay = 1
            ORDER BY acquired DESC
            '''

    cursor.execute(query)
    response = cursor.fetchall()
    cursor.close()
    cnx.close()

    activity_metrics = list(set(response))
    return dict([(int(float(am[0])), am[1]) for am in activity_metrics])

def extract_time_tryexcept(row):
    try:
        return datetime.strptime(row['ident'][9:15], "%H%M%S").time()
    except:
        try:
            return datetime.strptime(row['ident'][11:17], "%H%M%S").time()
        except:
            try:
                return datetime.strptime(row['ident'][7:13], "%H%M%S").time()
            except:
                try:
                    return datetime.strptime(row['ident'][8:14], "%H%M%S").time()
                except (ValueError, IndexError) as e:
                    # Handle the case where parsing fails or the string is too short
                    print(f"Error parsing time from row: {e}")
                    return None  # Or some default value
    

def cleanActMeasures_upd(loc, df, varsOfInterest): 
    # Set values to NA that exceed the median value per market, weekday of operation
    # and instrument by more than twice the IQR , calculated over the period 
    # outside Covid and for typical times and good images
    
    # engine = create_engine("mysql+mysqlconnector://root:BMkjM8_)-tN8R33u@34.72.234.161:3306/mai-database")
    dbActivity = pd.read_sql(f'SELECT * FROM activity_market WHERE Location = "{loc}"', engine).rename(columns = {'image_id':'ident'})
    dbActivity['time'] = dbActivity.apply(extract_time_tryexcept, axis=1)

    dbActivity['time_decimal'] = dbActivity['time'].apply(lambda t: t.hour + t.minute / 60 + t.second / 3600)
    #print(df.columns.tolist())
    df['median_time'] = df.groupby('instrument')['time_decimal'].transform('median')
    df['diff_to_median_time'] = abs(df['time_decimal'] - df['median_time'])
    maskdbA = (
        (dbActivity['date'].between('2020-03-01', '2021-02-28')) | # potentially covid-affected
        (dbActivity['date'] < '2018-01-01') |                      # generally noisier because of sparse imagery
        (dbActivity['diff_to_median_time'] > .5) |                  # differing sun angle
        ((dbActivity['clear_percent'].notnull()) & (dbActivity['clear_percent'] < 10)) | # noisy imagery
        ((dbActivity['cloud_percent'].notnull()) & (dbActivity['cloud_percent'] > 50))
    )
    mask = (
        (df['date'].between('2020-03-01', '2021-02-28')) | # potentially covid-affected
        (df['date'] < '2018-01-01') |                      # generally noisier because of sparse imagery
        (df['diff_to_median_time'] > .5) |                  # differing sun angle
        ((df['clear_percent'].notnull()) & (df['clear_percent'] < 10)) | # noisy imagery
        ((df['cloud_percent'].notnull()) & (df['cloud_percent'] > 50))
    )
    # Create a new column 'exclDates' based on the mask
    df['exclDates'] = mask.astype(int)
    dbActivity['exclDates'] = maskdbA.astype(int)
    metrics=getMostRecentActivityMetrics(loc)
    geos = list(set(metrics.values()))
    geos = [s.replace('sumsum_maxpMax_', '') for s in geos]
    geos = [re.sub(r'_(\d+)$', '_100', string) for string in geos]
    #print(geos)
    for b in geos: # within each possible area
        df[f'sumsum_maxpMax_{b}'] = df[f'sumsum_maxpMax_{b}'] / df[f'ccount_maxpMax_{b}'] # convert sum variable into mean deviations

        # Typical number of pixels per shape
        max_count = df.loc[df['exclDates'] != 1].groupby(['weekdayThisAreaIsActive', 'mktDay'])[f'ccount_maxpMax_{b}'].max().reset_index()
        df = pd.merge(df, max_count, on=[ 'weekdayThisAreaIsActive', 'mktDay'], how='outer', suffixes=('', '_max_count'))        

        for p in varsOfInterest:
            try: 
                # set to NA those values coming from images that cover less than 50% of the typical footprint
                df.loc[df[f'ccount_maxpMax_{b}']  < 0.5 *(df[f'ccount_maxpMax_{b}_max_count']), f'{p}_maxpMax_{b}'] = np.nan

                # calculate median, iqr by detected area and sensor, and merge to dataframe
                median = dbActivity.loc[dbActivity['exclDates'] != 1].groupby(['weekdayThisAreaIsActive', 'mktDay', 'instrument'])['activity_measure'].quantile(0.5).reset_index().rename(columns={'activity_measure': 'activity_measure_median'})
                df = pd.merge(df, median, on=[ 'weekdayThisAreaIsActive', 'mktDay', 'instrument'], how='outer', suffixes=('', '_median'))

                p25 = dbActivity.loc[dbActivity['exclDates'] != 1].groupby(['weekdayThisAreaIsActive', 'mktDay', 'instrument'])['activity_measure'].quantile(0.25)
                p75 = dbActivity.loc[dbActivity['exclDates'] != 1].groupby(['weekdayThisAreaIsActive', 'mktDay', 'instrument'])['activity_measure'].quantile(0.75)
                iqr = (p75-p25).reset_index().rename(columns={'activity_measure': 'activity_measure_iqr'})
                df = pd.merge(df, iqr, on=[ 'weekdayThisAreaIsActive', 'mktDay', 'instrument'], how='outer', suffixes=('', '_iqr'))
                # set to NA those values that are more than twice the IQR above the median
                df.loc[df[f'{p}_maxpMax_{b}']  > (df['activity_measure_median'] + 2 * df[f'activity_measure_iqr']), f'{p}_maxpMax_{b}'] = np.nan
                df = df.drop([f'activity_measure_median', f'activity_measure_iqr'], axis=1)    

            except Exception as e:
                print('Error in cleanActMeasures', e)
                pass
    df['mktDay']=df['mktDay'].astype(int)
    return df

def identify_varying_areas_upd(wide_df, activity_metrics, locGroup, loc): # Identify the largest ring in which P75 non-market day activity still does not exceed P50 market day activity
    gdfs = [] # dataframe to hold the selected shapes
    market_days = list(activity_metrics.keys())
    #print(market_days)
    if activity_metrics:
        #print(f'getting metrics from DB... {activity_metrics}')
        for market_day in market_days:
            activity_metric = activity_metrics[market_day]
            wide_df[f'maxVar_s_{int(market_day)}_maxpMax'] = activity_metric
            filtered_gdf = select_areas(int(market_day), activity_metric, locGroup, loc)
            gdfs.append(filtered_gdf)
            
        return wide_df, gdfs
    else:
        print(f"past activity metrics don't exist for {loc}")
        pass

def updateMAs(loc):
    
    # engine = create_engine("mysql+mysqlconnector://root:BMkjM8_)-tN8R33u@34.72.234.161:3306/mai-database")
    dbActivity = pd.read_sql(f'SELECT * FROM activity_market WHERE Location = "{loc}"', engine)
    dbActivity['weekday'] = dbActivity['weekday'].astype(float) 
    
    MAlist = []
    market_days = list(dbActivity[dbActivity['mktDay'] == 1]['weekday'].unique())
    for market_day in market_days:
        #aggregate by date and interpolate to calculate moving averages, using only SD instruments
        dfwd = dbActivity[dbActivity['weekday']==market_day]
        dfwd.index = dfwd['acquired']
        weeklyActSD = dfwd.groupby(["date"]).agg({'act_weekly': 'mean'}).interpolate('time')
        new_index = pd.date_range(weeklyActSD.index[0], weeklyActSD.index[-1], freq=f'W-{getWeekDayStr(market_day)}')
        MA = weeklyActSD.reindex(new_index)
        MA['act_1monthMA'] = weeklyActSD.interpolate('time').rolling(window=5, center=True).mean()
        MA['act_3monthMA'] = weeklyActSD.interpolate('time').rolling(window=13, center=True).mean()
        MA['date'] = MA.index
        MAlist.append(MA)
        
    MAfinal = pd.concat(MAlist, axis=0, ignore_index=True)
    dbActivity1 = pd.merge(dbActivity, MAfinal.drop('act_weekly', axis=1), on=['date'], how='outer', suffixes = ['_to_drop', ''])# 1 and 3-month moving average 
    dbActivity1 = dbActivity1.drop(['act_1monthMA_to_drop', 'act_3monthMA_to_drop'], axis = 1)
    dbActivity1.round(6).to_sql(con=engine, name='activity_market', if_exists='append', index=False, method=insert_on_duplicate)
    print(f'updated MAs for {loc}')

def updateRegionActivity(region, country):
    #pull all market activity data
    query = f'''
        SELECT * FROM `mai-database`.`activity_market`
        WHERE country = '{country}' AND admLvl1 = '{region}'
        AND mktDay = 1
        LIMIT 10000000
        '''
    #engine = create_engine("mysql+mysqlconnector://root:BMkjM8_)-tN8R33u@34.72.234.161:3306/mai-database")
    df = pd.read_sql(query, con=engine)
    
    #group into weeks, dropping any groups that have too few market observations
    df['date'] = pd.to_datetime(df['date'])
    minCount = len(df['Location'].unique())/3
    grouped = df.groupby(pd.Grouper(freq='W-SUN', key='date'))
    cleaned = grouped.filter(lambda x: x['act_weekly'].count() > minCount) #make sure that day has enough observations
    final = cleaned.groupby(pd.Grouper(freq='W', key='date')).agg({'act_weekly':'mean',
                                                                   'act_1monthMA':'mean',
                                                                   'act_3monthMA' : 'mean',
                                                                   'admLvl1':'first',
                                                                   'country':'first'})
    final['date'] = final.index
    
    #upload to SQL
    final.to_sql(con=engine, name='activity_region', if_exists='append', index=False, method=insert_on_duplicate, chunksize = 100)
    logger.debug(f'uploaded activity data for {region} in {country}')
    
def updateCountryActivity(country):
    #pull all market activity data
    query = f'''
        SELECT * FROM `mai-database`.`activity_market`
        WHERE country = '{country}'
        AND mktDay = 1
        LIMIT 10000000
        '''
    #engine = create_engine("mysql+mysqlconnector://root:BMkjM8_)-tN8R33u@34.72.234.161:3306/mai-database")
    df = pd.read_sql(query, con=engine)

    #group into weeks, dropping any groups that have too few market observations
    df['date'] = pd.to_datetime(df['date'])
    minCount = len(df['Location'].unique())/4
    grouped = df.groupby(pd.Grouper(freq='W-SUN', key='date'))
    cleaned = grouped.filter(lambda x: x['act_weekly'].count() > minCount) #make sure that day has enough observations
    final = cleaned.groupby(pd.Grouper(freq='W', key='date')).agg({'act_weekly':'mean',
                                                                   'act_1monthMA':'mean',
                                                                   'act_3monthMA' : 'mean',
                                                                   'country':'first'})
    final['date'] = final.index

    #upload to SQL
    final.to_sql(con=engine, name='activity_country', if_exists='append', index=False, method=insert_on_duplicate, chunksize = 100)
    logger.debug(f'uploaded activity data for {country}')
    
def delete_false_positives(loc):
    GEEbucket = checkLocationFileStatus(loc, 'bucket')
    locGroup = checkLocationFileStatus(loc, 'locGroup')
    bucketFolder = ee.data.listAssets(f'projects/{GEEbucket}/assets/PS_imgs')['assets'][0]['id'].split('/')[-1]
    
    # delete images 
    command = f'earthengine rm -r "projects/{GEEbucket}/assets/PS_imgs/{bucketFolder}/{loc}"'
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')

    cursor = cnx.cursor()
    queries = [
        f"DELETE FROM activity_market WHERE Location = '{loc}';",
        f"UPDATE process_runs SET Status='deleted' WHERE Location = '{loc}' AND Setup IN ('exportAct5','MpM6');"
    ]

    for query in queries:
        cursor.execute(query)
        cnx.commit()
    cnx.close()
    cursor.close()
    
def delete_duplicates(loc):
    toApp = []
    unique_matches = set()
    # Assuming these functions are defined elsewhere in your code
    bucket = checkLocationFileStatus(loc, 'bucket')
    locGroup = checkLocationFileStatus(loc, 'locGroup')
    
    root_name = f"projects/{bucket}/assets/PS_imgs/{locGroup}/" 
    try:
        folder_name = root_name + loc
        asset_list = ee.data.getList({'id': folder_name})
        id_list = [item['id'] for item in asset_list]
        
        for asset in id_list:
            assetID = asset.replace(folder_name, "").replace('/', '')
            match = re.search(r'^(.*?)_Analytic', assetID)
            if match:
                match_str = match.group(1)
                unique_matches.add(match_str)
                toApp.append(match_str)
        
        duplicates = [match for match in list(unique_matches) if sum(1 for id_item in id_list if match in id_item) > 1]
        
        # Print the duplicates
        if len(duplicates)>0:
            print("Duplicates of the string before '_Analytic':", len(duplicates), loc, root_name)

            for duplicate in sorted(duplicates):
                # List all assets with this ID
                result = [item for item in id_list if duplicate in item]
                for res in result[1:]:  # Delete the second result and onwards
                    ee.data.deleteAsset(res)
                    # print(f'deleted asset {res}')
        updateLocationFileStatus(loc, "lastImageDedupe", datetime.today().strftime("%Y-%m-%d"), replace=True)

    except Exception as e:
        error_message = str(e)
        if "not found" not in error_message:
            print(f"Error processing location {loc}: {error_message}")    
    
def parallel_process_locations(locs, function):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(function, loc) for loc in locs]
        
        # Wait for all futures to complete and handle exceptions
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                error_message = str(e)
                if "not found" not in error_message:
                    print(f"Error occurred: {error_message}")

def clear_local_dataframes(locals_dict):
    """
    Function to delete all DataFrame objects from the local context.
    
    Args:
    locals_dict (dict): Pass locals() dictionary from the function where you want to clear DataFrames.
    """
    # Create a list of names of local variables that are DataFrames to avoid modifying the dictionary size during iteration
    to_delete = [name for name, item in locals_dict.items() if isinstance(item, pd.DataFrame)]
    # Delete each DataFrame
    for name in to_delete:
        del locals_dict[name]
    # Run garbage collection
    gc.collect()  
    
def extract_ident_base(ident):
    # Extracts pattern: 6 digits + "_" + 6 digits + "_" + 1 digits
    match = re.search(r'(\d{6}_\d{6}_\d{1})', str(ident))
    return match.group(1) if match else None

def market_days_from_location_file(loc):
    mktDays = pd.read_sql(f"SELECT marketDays FROM location_file WHERE Location = '{loc}'", engine)
    return [int(d) for d in list(mktDays.iloc[0,0])]

def upload_df_to_gcs(df, file_path):
    """
    Uploads a pandas DataFrame to GCS as a CSV file using gcsfs.
    Verifies success by comparing file size before and after upload.

    Parameters:
    - df: pandas.DataFrame
    - file_path: str, full GCS path (e.g., 'gs://my-bucket/folder/file.csv')
    """
    fs = gcsfs.GCSFileSystem()

    # Get size before upload (if file exists)
    prev_size = fs.info(file_path)['size'] if fs.exists(file_path) else None

    # Write to GCS
    try:
        with fs.open(file_path, 'w') as f:
            df.to_csv(f, index=False)
    except Exception as e:
        print(f"Upload failed due to error: {e}")
        return False

    # Get size after upload
    new_size = fs.info(file_path)['size'] if fs.exists(file_path) else None

    # Verify success
    if new_size is not None and (prev_size is None or new_size != prev_size):
        print(f"Upload successful: {file_path} ({new_size} bytes)")
        return True
    else:
        print(f"Upload may have failed or file was unchanged (size = {new_size})")
        return False

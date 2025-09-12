import os
import pandas as pd
import numpy as np
import geopandas as gpd
from datetime import datetime
from scipy import interpolate
import matplotlib.pyplot as plt
import re

propToDrop=['quality_category','system:index', '.geo','order_id', 'pixel_resolution','gsd','provider', 'published', 'publishing_stage', 'item_type', 'item_id', 'snow_ice_percent', 'strip_id','updated']
maxRank = 4 # exclude altitude levels above this
varsOfInterest=['sumsum', 'ccount']
forMerge=['ident','weekdayThisAreaIsActive','date','mktDay','mktID','locGroup','time','year','month', 'weekday', 'mkt_lat','mkt_lon','time_decimal'] 
patterns_to_drop = ['ground_control','strictnessRank', 'subStrictnessRank''Geography','origName_', 'coorLength_', '.geo', 'system:index_b0', 'system:index', 'weekday_','market']
_startDateNorm='2018-01-01'
_endDateNorm='2018-12-31'


def prepare_properties(locGroup, loc, propToDrop):  
    
    prop_path = os.path.join('..', 'datasets', 'intermediate_outputs', f'{locGroup}_properties_propEx_{locGroup}_{loc}.csv')
    df_prop = pd.read_csv(prop_path)
    # Extract 'ident' from 'system:index' column
    df_prop['ident'] = df_prop['system:index'].str.slice(stop=23) 
    # Determine the imagery generation of each image
    df_prop['instrument'] = df_prop.apply(determine_sensor, axis=1)
        
    # Drop specified properties from the DataFrame
    for prop in propToDrop:
        try:
            df_prop = df_prop.drop(prop, axis=1)
        except KeyError:
            pass
    return df_prop  

def determine_sensor(row):
    image_id = row['ident']
    condition1 = '3B' in image_id[-2:]
    condition2 = '_1_' in image_id
    if condition1 or condition2:
        return 'PS2'
    else:
        return 'PSB.SD'
    
def prepend_zero_if_single_digit(value):
    if len(str(value)) == 1:
        return '0' + str(value)
    else:
        return str(value)

def infoVars(df, mktID, locGroup, country): # assign info variables based on date and location
    df['mktID'] = mktID
    df['locGroup'] = locGroup
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

def identifyMktDays(loc, df, minRank): # identify market days based on detected areas and their threshold values
    
    # List all maximum threshold values on the days-of-week where we detected something and that detection falls below a threshold 
    min_thres_by_day = df.groupby('weekdayThisAreaIsActive')['strictnessRank'].min()
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

def drop_columns_by_pattern(df, patterns_to_drop):
    for pattern in patterns_to_drop:
        try:
            df = df.drop(df.filter(like=pattern).columns, axis=1)
        except Exception as e:
            print(f"Error occurred while dropping columns for pattern '{pattern}': {e}")
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

def identify_varying_areas(wide_df, locGroup,loc): # Identify the largest ring in which P75 non-market day activity still does not exceed P50 market day activity
    market_days = wide_df.loc[wide_df['mktDay'] == 1, 'weekday'].unique().tolist()
    valid = False
    print('market_days_', market_days)
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
        return wide_df, gdfs, market_days
    else:
        return pd.DataFrame(),pd.DataFrame()

def select_areas(market_day, first_row_index, locGroup, loc): #select the shapes associated with the selected market area
    # extract substring between second last and last instance of _
    temp = first_row_index.split('_')
    if len(temp) >= 2:
        minRing =  int(temp[-2])
    else:
        minRing = None  # Return None if there aren't enough parts
    # load shapefile using relative path
    shp_path = os.path.join('..', 'datasets', 'intermediate_outputs', f'{locGroup}_shapes_shp_MpM6_{locGroup}{loc}.shp')
    gdf = gpd.read_file(shp_path)    
    filtered_gdf = gdf[(gdf['weekdayShp'] == market_day) & 
                   (gdf['strictness'] == minRing) & 
                   (gdf['subStrictn'] == 100)].copy()
    #filtered_gdf.plot()
    filtered_gdf.loc[:, 'mktid'] = loc  # Use .loc to set values
    return filtered_gdf

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
    allActivity = allActivity[allActivity['instrument'] == 'PS2'].dropna(subset=['activity_measure'])
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
        print(f'Warning: difference between smoothed mean and simple mean for {loc} exceeds the standard deviation.')
        # updateProcess(loc, 'activityUpload', '', 'Large means difference in activity upload', column = 'notes')

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


def replace_after_underscore(s):
    return s[:s.rfind('_') + 1] + '100'


def activity_processor(loc, GEEbucket, locGroup, country):
    # Since we're now using relative paths, we don't need repl_pkg_path parameter
    #locCount=0
    #print(f'Uploading activity for {loc}...')
    #if checkProcessStatus(loc, "activityUpload", setup='activityUpload') == None:
    #    startProcess(loc, "activityUpload", setup='')
    #filePath = f'gs://exports-mai2023/activity_cleaned_2024/df_{loc}.csv'
    #locCount = locCount+1
    #try:
    #    raise Exception('Skipping intentionally')
    #    df = pd.read_csv(filePath)
    #    market_shapes = gpd.read_file(f"gs://exports-mai2023/{target_folder}/shp_{loc}.shp", driver='ESRI Shapefile')
    #    if 'PS2.SD' in list(df['instrument']):
    #        print('Instruments not correctly re-categorized the first time – recreating csv for upload...')
    #        raise Exception("PS2.SD should not exist in the uploaded CSV")

    #except:  
    #try: 
    # prepare image property dataframe to be merged in later
    df_prop = prepare_properties(locGroup, loc, propToDrop)

    # Read the activity CSV file
    df = pd.read_csv(os.path.join('..', 'datasets', 'intermediate_outputs', f'{locGroup}_measures_exportAct5_maxpMax{loc}_w7.csv'))
 
    # keep only entries that fall between the strictest rank we define and the least strict one for a given shape, but at least 30
    minRank = max(df['strictnessRank'].min(),30)
    df = df[(df['strictnessRank'] <= minRank) & (df['strictnessRank'] >= maxRank)]
    df = df[((df['subStrictnessRank'] <= minRank) & (df['subStrictnessRank'] > maxRank)) | (pd.isna(df['subStrictnessRank'])) | (df['subStrictnessRank'] ==100)]
 
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
    df_elig = infoVars(df_elig, loc, locGroup, country)

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
    df, market_shapes_list, market_days = identify_varying_areas(wide_df, locGroup,loc)

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

    df = pd.merge(df, mean_nonmktday, on=['weekdayThisAreaIsActive', 'instrument'], how='outer', suffixes=('', '_mean_nonmktday'))
    df['activity_measure_mean0'] = df['activity_measure'] - df['activity_measure_mean_nonmktday']
    df = pd.merge(df, mean_mktday, on=['weekdayThisAreaIsActive', 'instrument'], how='outer', suffixes=('', '_mean_mktday'))
    df['activity_measure_norm'] = 100*df['activity_measure_mean0']/df['activity_measure_mean0_mean_mktday'] 
    
    # Apply the function to each string in listA
    tokeep_100 = [replace_after_underscore(col) for col in tokeep]        
    cols_to_drop = [col for col in df.columns if ('maxVar' not in col and 'maxpMax' in col) and col not in tokeep and col not in tokeep_100 ]
    #print(cols_to_drop)
    df = df.drop(columns=cols_to_drop)
    df = df.drop(columns=['ground_control', 'time', 'locGroup'])

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

    df = df.rename(columns={"ident": "image_id"})

    #update df with variables wanted in database
    df['Location'] = loc
    df['acquired'] = pd.to_datetime(df['acquired'], utc=True, errors='coerce')

    return df

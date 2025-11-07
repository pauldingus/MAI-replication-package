from IPython.display import display, clear_output
import pandas as pd
import ee, mysql.connector, subprocess
from sqlalchemy import create_engine
from MAI2023.src.dbFunctions import *
ee.Initialize()


# ------------------------------------------------------------------------------------------------------------------------------ 
# BUCKET MANAGEMENT FUNCTIONS 
# ------------------------------------------------------------------------------------------------------------------------------
      
    
def projects_to_dataframe():

    # Run the gcloud command and capture the output
    command = 'gcloud projects list --filter="PROJECT_ID ~ ^p\\d+\\w"'
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    output = result.stdout.strip().split("\n")

    # Split each line into columns and create a list of dictionaries
    data = []
    for line in output[1:]:  # Skip the header row
        cols = line.split()
        data.append({
            "PROJECT_ID": cols[0],
            "NAME": cols[1],
            "PROJECT_NUMBER": cols[2]
        })

    # Convert the data into a Pandas DataFrame
    return pd.DataFrame(data)



def dbBucket_status():

    query = f'''
            SELECT bucket,
                count(*) as totalLocations, 
                count(if(`00DownStatus` != '', 1, NULL)) as startedLocations
            FROM `mai-database`.`location_file`
            WHERE to_delete IS NULL
            GROUP BY bucket ORDER BY bucket;
            '''
    engine = create_engine("mysql+mysqlconnector://root:BMkjM8_)-tN8R33u@34.72.234.161:3306/mai-database")
    return pd.read_sql(query, engine)



def get_project_quotas(projectIDs):

    quotaList = []
    for projectID in projectIDs:
        try:
            quotaInfo = ee.data.getAssetRootQuota(f'projects/{projectID}/assets/')
            
            #convert to flat dict
            quota = dict()
            quota['maxSizeBytes'] = quotaInfo['asset_size']['limit']
            quota['sizeBytes'] = quotaInfo['asset_size']['usage']
            quota['maxAssets'] = quotaInfo['asset_count']['limit']
            quota['assetCount'] = quotaInfo['asset_count']['usage']

            #include useful info
            quota['freeBytes'] = quota['maxSizeBytes'] - quota['sizeBytes']
            quota['pctFreeBytes'] = round(100*(quota['freeBytes'])/quota['maxSizeBytes'],2)

            quota['freeAssetCount'] = quota['maxAssets'] - quota['assetCount']
            quota['pctFreeAssets'] = round(100*(quota['freeAssetCount'])/quota['maxAssets'],2)

            quota['projectID'] = projectID
            quotaList.append(quota)
        
        except Exception as e:
            print(f'Failed to access project quota for {projectID}: {e}')
        
    return pd.DataFrame.from_dict(quotaList) 



def get_project_quota_summary():
    allProjects = projects_to_dataframe()
    dbProjects = dbBucket_status()
    dbProjects = dbProjects[~dbProjects.bucket.isin([None, ''])]
    projectStatus = allProjects.merge(dbProjects, left_on = 'PROJECT_ID', right_on = 'bucket', how = 'outer')
    projectStatus['PROJECT_ID'] = projectStatus['PROJECT_ID'].fillna(projectStatus['bucket'])
    projectStatus = projectStatus \
        .rename(columns = {'PROJECT_ID':'projectID'}) \
        .fillna({'totalLocations':0, 'startedLocations':0}) \
        .drop(['bucket', 'NAME', 'PROJECT_NUMBER'], axis = 1)
    
    projectQuotas = get_project_quotas(projectStatus.projectID.unique())
    
    return projectQuotas.merge(projectStatus, on = 'projectID', how = 'outer')



def remove_unstarted_location(loc):
    
    try:

        locGroup = checkLocationFileStatus(loc, 'locGroup')
        bucket = checkLocationFileStatus(loc, 'bucket')

        if ee.data.listAssets(f'projects/{bucket}/assets/PS_imgs/{locGroup}/{loc}')['assets']:
            print(f'Images already exist for loc {loc} -- aborting.')
            return

        #delete old image collection
        ee.data.deleteAsset(f'projects/{bucket}/assets/PS_imgs/{locGroup}/{loc}')

        #delete old processed folder
        ee.data.deleteAsset(f'projects/{bucket}/assets/PS_imgs/{locGroup}/{loc}proc')

        #check whether assets were deleted correctly
        assetList = ee.data.listAssets(f'projects/{bucket}/assets/PS_imgs/{locGroup}')['assets']
        if not [asset['id'] for asset in assetList if loc in asset['id']]:
            print(f'successfully removed {loc} from bucket {bucket}')
        else:
            print(f'assets not successfully removed for {loc} in bucket {bucket} -- aborting')
            print(f'remaining asset list: {assetList}')

        #update bucket in database to reflect changes
        cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')
        cursor = cnx.cursor()
        query = (f"UPDATE `mai-database`.location_file SET bucket = '', locGroup = '' WHERE Location = '{loc}';")
        cursor.execute(query)
        cnx.commit()
        cnx.close()
        cursor.close()
    
    except Exception as e:
        print(f'Failed to remove {loc}: {e}')
        
        
        
def find_emptiest_bucket(quotaSummary = None, minQuotaPerLoc = 3000):
    print('Finding emptiest bucket...')

    if not quotaSummary:
        quotaSummary = get_project_quota_summary()

    quotaSummary = quotaSummary[quotaSummary['freeAssetCount'] > minQuotaPerLoc]
    quotaSummary['quotaPerLoc'] = quotaSummary['maxAssets']/quotaSummary['totalLocations']
    quotaSummary = quotaSummary[quotaSummary['quotaPerLoc'] >= minQuotaPerLoc]
    quotaSummary = quotaSummary[quotaSummary['freeAssetCount'] >= 2 * minQuotaPerLoc]
    
    # Find the minimum 'quotaPerLoc' value
    #min_quota_per_loc = quotaSummary['quotaPerLoc'].max()
    #print('min_quota_per_loc',min_quota_per_loc)
    # Filter to only entries with this minimum value
    #emptiest_buckets = quotaSummary[quotaSummary['quotaPerLoc'] > min_quota_per_loc]
    display(quotaSummary)
    # Randomly select one of the rows with the minimum 'quotaPerLoc'
    return quotaSummary.sample(n=1)['projectID'].values[0]


def add_loc_to_bucket(loc):
    
    bucketCurr = checkLocationFileStatus(loc, 'bucket')
    
    #abort if bucket is already assigned
    if bucketCurr:
        raise Exception(f'bucket must be unassigned. Current assignments for {loc}: {bucketCurr}')
    
    emptiestBucket = find_emptiest_bucket()
    bucket = emptiestBucket['projectID']
    print('emptiestBucket',emptiestBucket)
    locGroupDest = ee.data.listAssets(f'projects/{bucket}/assets/PS_imgs')['assets'][0]['id'].split('/')[-1]
    
    #create new loc folder in destination bucket
    try:
        ee.data.createAsset({'type': 'Folder'}, f'projects/{bucket}/assets/PS_imgs/{locGroupDest}/{loc}proc')
    except ee.ee_exception.EEException as e:
        if 'Cannot overwrite asset' in str(e):
            pass
        else:
            raise e
        
    #create new image collection in destination bucket
    try:
        ee.data.createAsset({'type': 'ImageCollection'}, f'projects/{bucket}/assets/PS_imgs/{locGroupDest}/{loc}')
    except ee.ee_exception.EEException as e:
        if 'Cannot overwrite asset' in str(e):
            pass
        else:
            raise e
            
    #check whether assets were created correctly
    assetList = ee.data.listAssets(f'projects/{bucket}/assets/PS_imgs/{locGroupDest}')['assets']
    if [asset['id'] for asset in assetList if loc in asset['id']]:
        print(f'successfully added {loc} to bucket {bucket}')
    else:
        raise Exception(f'failed to add {loc} to bucket {bucket}')
        
    updateLocationFileStatus(loc, "bucket", bucket, replace=True)
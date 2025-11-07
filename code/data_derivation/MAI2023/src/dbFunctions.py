# ------------------------------------------------------------------------------------------------------------------------------ 
# LOCATION FILE MANAGEMENT FUNCTIONS 
# ------------------------------------------------------------------------------------------------------------------------------

import mysql.connector, logging, threading
import pandas as pd
from sqlalchemy import create_engine
logger = logging.getLogger('logging')
db_lock = threading.Lock()

def checkLocationFileStatus(loc, columns):
    #function to check the value of given and tracking column within the master location file
    #cnx:      mySQL connection object
    #locGroup: string (e.g: "79_Tigray_1")
    #loc:      string (e.g: "lon14_115lat38_4743")
    #column:   string (e.g: "00DownStatus")
    #returns:  value in the specified column for the loc 
    
    cnx = None
    cursor = None
    
    if type(columns) == list:
        columnString = ', '.join(columns)
    else:
        columnString = columns
        
    try:
        with db_lock:
    
            cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')

            cursor = cnx.cursor()
            query = (f"SELECT {columnString} FROM `mai-database`.location_file WHERE Location = '{loc}'")
            cursor.execute(query)
            response = cursor.fetchall()
            if type(columns) == list:
                return [row for row in response][0]
            else:
                return [row for row in response][0][0]
            
    except mysql.connector.Error as error:
        print(f"Error updating JSON value: {error}")
        
    finally:
        # Close the cursor and connection in the finally block
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()

    
def updateLocationFileStatus(loc, column, value, replace = False, remove = False):
    #function to update the value of a given and tracking column within the master location file
    #if old_value is specified, the operation will fail unless the existing value in the csv matches old_value
    #locGroup:  string (e.g: "79_Tigray_1")
    #loc:       string (e.g: "lon14_115lat38_4743")
    #column:    string (e.g: "00DownStatus")
    #old_value: string (e.g: "complete")
    #new_value: string (e.g: "initiated")
    
    cnx = None
    cursor = None
    try:
        with db_lock:
    
            cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')

            if replace:
                query = (f"UPDATE `mai-database`.location_file SET {column} = '{value}' WHERE Location = '{loc}';")
            else:
                current = checkLocationFileStatus(loc, column)
                if not remove:
                    if value not in current:
                        if current in ' ':
                            new = value
                        else:
                            new = current + ', ' + value
                        query = (f"UPDATE `mai-database`.location_file SET {column} = '{new}' WHERE Location = '{loc}';")
                    else:
                        return
                else:
                    if value not in current:
                        return
                    else:
                        new = current.replace(', ' + value, '').replace(value, '')
                    query = (f"UPDATE `mai-database`.location_file SET {column} = '{new}' WHERE Location = '{loc}';")

            cursor = cnx.cursor()        
            cursor.execute(query)
            cnx.commit()
            
    except mysql.connector.Error as error:
        print(f"Error updating JSON value: {error}")
        
    finally:
        # Close the cursor and connection in the finally block
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()
    

def startProcess(loc, process, setup):
    
    cnx = None
    cursor = None
    
    if checkProcessStatus(loc, process, setup):
        
        print(f'Restarting process record that already exists: {process} for {loc} with setup {setup} -- resetting start time.')
        try:
            with db_lock:

                cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')

                query = f'''
                UPDATE `mai-database`.process_runs 
                SET StartTime = NOW()
                WHERE Location = '{loc}'
                AND Process = '{process}'
                AND Setup = '{setup}'
                '''
                cursor = cnx.cursor()
                cursor.execute(query)
                cnx.commit()
                
        except:
            logger.debug(f'ERROR: could not create a process for {loc}:')
            logger.debug(f'query: {query}')
            return None
        
        finally:
            # Close the cursor and connection in the finally block
            if cursor:
                cursor.close()
            if cnx:
                cnx.close()
        return
    
    try:
        with db_lock:
    
            cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')

            query = f'''
            INSERT INTO `mai-database`.process_runs (Location, Process, Setup, Status, StartTime)
            VALUES (
                '{loc}',
                '{process}',
                '{setup}',
                'started',
                NOW()
            )
            '''
            cursor = cnx.cursor()
            cursor.execute(query)
            cnx.commit()
            
    except:
        logger.debug(f'ERROR: could not create a process for {loc}:')
        logger.debug(f'query: {query}')
        return None
    
    finally:
        # Close the cursor and connection in the finally block
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()
    

def updateProcess(loc, process, setup, value, fail_reason=None, column = 'Status'):
    
    cnx = None
    cursor = None
    try:
        with db_lock:
    
            cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')

            query = f'''
            UPDATE `mai-database`.process_runs
            SET {column} = '{value}', EndTime = NOW(), FailReason = '{fail_reason}'
            WHERE Location = '{loc}'
            AND Process = '{process}'
            AND Setup = '{setup}'
            '''
            cursor = cnx.cursor()
            cursor.execute(query)
            cnx.commit()
    except:
        logger.debug(f'ERROR: could not update a process for {loc}:')
        logger.debug(f'query: {query}')
        return None
    
    finally:
        # Close the cursor and connection in the finally block
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()


def checkProcessStatus(loc, process, setup, column = 'Status', returnLength=False):
    cnx = None
    cursor = None
    try:
        with db_lock:
    
            cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')

            query = f'''
            SELECT {column} FROM `mai-database`.process_runs
            WHERE Location = '{loc}'
            AND Process = '{process}'
            AND Setup = '{setup}'
            '''

            cursor = cnx.cursor()
            cursor.execute(query)
            response = cursor.fetchall()

            cursor.close()
            if response:
                if returnLength:
                    return len(response)
                else:
                    return [row for row in response][0][0]
            else:
                return None
    
    except:
        logger.debug(f'Error: could not check a process for {loc}:')
        logger.debug(f'query: {query}')
        
    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()
        

def getAssignedLocs(name, setupImg, setupMap, setupActivityPrep, setupActivity, status = '', prepExportSuffix = '', rerun = 0):
    #returns a list of locations from the database which are assigned to the input name
    cnx = None
    cursor = None
    try:
        with db_lock:
            cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')

            if status == '':
                query = f"SELECT `Location` FROM `mai-database`.location_file WHERE `Assignment` = '{name}' "

            if status == 'for_download':
                query = f'''
                SELECT `Location` FROM `mai-database`.location_file 
                WHERE `Assignment` = '{name}' 
                AND  `to_delete` IS NULL
                AND (`00DownStatus` NOT IN ('complete', 'failed', 'updating')
                OR JSON_EXTRACT(`totalAvailable`, '$."cloud_0"') IS NULL
                OR JSON_EXTRACT(`totalAvailable`, '$."cloud_25"') IS NULL
                OR JSON_EXTRACT(`totalAvailable`, '$."cloud_50"') IS NULL
                OR JSON_EXTRACT(`totalAvailable`, '$."cloud_75"') IS NULL
                OR JSON_EXTRACT(`totalAvailable`, '$."cloud_100"') IS NULL)
                ORDER BY mktPrediction DESC, maxSignal DESC
                '''                
                
            if status == 'for_process':
                query = f'''
                SELECT l.Location
                FROM `mai-database`.`location_file` l
                WHERE l.Assignment = '{name}'
                AND  `to_delete` IS NULL
                AND `00DownStatus` = 'complete' 
                AND (false_positive=0 OR false_positive IS NULL)
                AND NOT EXISTS (
                        SELECT 1
                        FROM `mai-database`.`process_runs` pr
                        WHERE pr.Location = l.Location
                        AND pr.Process = '02Map'
                        AND pr.Setup = '{setupMap}'
                        AND pr.minStrictnessRank IS NOT NULL
                        AND pr.minStrictnessRank > 20
                        AND (pr.runAnyway IS NULL OR pr.runAnyway="no")
                        )
                AND NOT EXISTS (
                        SELECT 1
                        FROM `mai-database`.`process_runs` pr
                        WHERE pr.Location = l.Location
                        AND pr.Process IN ('01Prep', '02Map', '03ActivityPrep' ,'04ActivityExport')
                        AND pr.Setup IN ('{setupImg}', '{setupMap}', '{setupActivityPrep}', '{setupActivity}')
                        AND pr.Status = 'failed'
                        )
                AND (
                    NOT EXISTS (
                        SELECT 1
                        FROM `mai-database`.`process_runs` pr
                        WHERE pr.Location = l.Location
                        AND pr.Process = '01Prep'
                        AND pr.Setup = '{setupImg}'
                        AND pr.Status IN ('complete', 'failed')
                    )
                    OR
                    NOT EXISTS (
                        SELECT 1
                        FROM `mai-database`.`process_runs` pr
                        WHERE pr.Location = l.Location
                        AND pr.Process = '02Map'
                        AND pr.Setup = '{setupMap}'
                        AND pr.Status IN ('complete', 'failed')
                    )
                    OR
                    NOT EXISTS (
                        SELECT 1
                        FROM `mai-database`.`process_runs` pr
                        WHERE pr.Location = l.Location
                        AND pr.Process = '03ActivityPrep'
                        AND pr.Setup = '{setupActivityPrep}'
                        AND pr.Status IN ('complete', 'failed')
                    )
                    OR
                    NOT EXISTS (
                        SELECT 1
                        FROM `mai-database`.`process_runs` pr
                        WHERE pr.Location = l.Location
                        AND pr.Process = '04ActivityExport'
                        AND pr.Setup = '{setupActivity}'
                        AND pr.Status IN ('complete', 'failed')
                    )
                    OR
                    NOT EXISTS (
                        SELECT 1
                        FROM `mai-database`.`process_runs` pr
                        WHERE pr.Location = l.Location
                        AND pr.Process = 'PropExport'
                        AND pr.Status IN ('complete', 'failed')
                    )
                )
                '''
#                 print('process query')
#                 print(query)
                
                
            cursor = cnx.cursor()
            cursor.execute(query)
            response = cursor.fetchall()
            return [row[0] for row in response]
    
    except:
        logger.debug(f'Error: could attain assigned locs for {name}')
        logger.debug(f'query: {query}')
        return None  
    
    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()
    
def getAssignedLocs_8B(name, setupImg, setupMap, setupActivityPrep, setupActivity, status = '', prepExportSuffix = '', rerun = 0):
    #returns a list of locations from the database which are assigned to the input name
    cnx = None
    cursor = None
    try:
        with db_lock:
            cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')

            if status == '':
                query = f"SELECT `Location` FROM `mai-database`.location_file WHERE `Assignment` = '{name}' "

            if status == 'for_download':
                query = f'''
                SELECT `Location` FROM `mai-database`.location_file 
                WHERE `Assignment` = '{name}' 
                AND  `to_delete` IS NULL
                AND (`00DownStatus_8B` NOT IN ('complete', 'failed', 'updating')
                OR JSON_EXTRACT(`totalAvailable`, '$."cloud_0"') IS NULL
                OR JSON_EXTRACT(`totalAvailable`, '$."cloud_25"') IS NULL
                OR JSON_EXTRACT(`totalAvailable`, '$."cloud_50"') IS NULL
                OR JSON_EXTRACT(`totalAvailable`, '$."cloud_75"') IS NULL
                OR JSON_EXTRACT(`totalAvailable`, '$."cloud_100"') IS NULL)
                ORDER BY mktPrediction DESC, maxSignal DESC
                '''                

                
            cursor = cnx.cursor()
            cursor.execute(query)
            response = cursor.fetchall()
            return [row[0] for row in response]
    
    except:
        logger.debug(f'Error: could attain assigned locs for {name}')
        logger.debug(f'query: {query}')
        return None  
    
    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()
    


def locationFileSummary(loc = 'unspecified', locGroup = 'unspecified'):
    #function to display the row associated with a loc in the master location file
    #if loc is unspecified, returns the entire location file as a dataframe
    #locGroup: string (e.g: "79_Tigray_1")
    #loc:      string (e.g: "lon14_115lat38_4743")
    #locGroup, Location, bucket, 00DownStatus, 00aDownNoSRStatus, 01PrepStarted, 01aPrepComplete, 01bPrepFailed, 02MapStarted, 02aMapComplete, 02bMapFailed, 03ActStarted, 03aActComplete, 03bActFailed, 03DiffImgStarted, 03aDiffImgComplete, 03bDiffImgFailed, 04PropertiesDownload, Assignment
    cnx = None
    cursor = None
    try:
        with db_lock:
            
            cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')

            if loc != 'unspecified':

                cursor = cnx.cursor()
                cursor.execute(f"SELECT * FROM `location_file` WHERE `Location` = '{loc}'")
                response = cursor.fetchall()
                column_titles = [i[0] for i in cursor.description]
                df = pd.DataFrame(response, columns=column_titles)

            elif locGroup != 'unspecified':
                cursor = cnx.cursor()
                cursor.execute(f"SELECT * FROM `location_file` WHERE `locGroup` = '{locGroup}'")
                response = cursor.fetchall()
                column_titles = [i[0] for i in cursor.description]
                df = pd.DataFrame(response, columns=column_titles)

            else:
                raise Exception('Either loc or locGroup must be specified.')

    except mysql.connector.Error as error:
        print(f"Error updating JSON value: {error}")

    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()


def updateJSON(loc, column, key_name, value):
    # Establish a connection to the MySQL database
    cnx = None
    cursor = None
    try:
        with db_lock:
            
            cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')

            query = f'''
            UPDATE `mai-database`.location_file 
            SET `{column}` = JSON_SET(COALESCE(`{column}`, '{{}}'), '$.{key_name}', {value}) 
            WHERE Location = "{loc}"
            '''

            cursor = cnx.cursor()
            cursor.execute(query)
            cnx.commit()
            logger.debug(f"JSON value updated successfully for {key_name} in {loc}")

    except mysql.connector.Error as error:
        print(f"Error updating JSON value: {error}")

    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()


def getJSON(loc, column, key_name):
    # Establish a connection to the MySQL database
    cnx = None
    cursor = None
    try:
        with db_lock:
            
            cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')

            query = f'''
            SELECT JSON_UNQUOTE(JSON_EXTRACT({column}, '$.{key_name}')) 
            FROM `mai-database`.location_file 
            WHERE Location = "{loc}"
            '''

            cursor = cnx.cursor()
            cursor.execute(query)
            result = cursor.fetchone()

            if result is not None and result[0] is not None:
                value = result[0]
                return value
            else:
                logger.debug(f"No value found for {key_name} in column {column} for {loc}")
                return None

    except mysql.connector.Error as error:
        print(f"Error updating JSON value: {error}")
        return None

    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()   


def startUpdateProcess(loc, process, setup, dateID, startDate = 'NULL', endDate = 'NULL'):
    
    cnx = None
    cursor = None
    
    if checkUpdateProcessStatus(loc, process, setup, dateID):
        
        print(f'Restarting process record that already exists: {process} for {loc} with setup {setup} -- resetting start time.')
        try:
            with db_lock:

                cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')

                query = f'''
                UPDATE `mai-database`.process_runs 
                SET StartTime = NOW()
                WHERE Location = '{loc}'
                AND Process = '{process}'
                AND Setup = '{setup}'
                AND dateID = '{dateID}'
                '''
                cursor = cnx.cursor()
                cursor.execute(query)
                cnx.commit()
                
        except:
            logger.debug(f'ERROR: could not create a process for {loc}:')
            logger.debug(f'query: {query}')
            return None
        
        finally:
            # Close the cursor and connection in the finally block
            if cursor:
                cursor.close()
            if cnx:
                cnx.close()
        return
    
    try:
        with db_lock:
    
            cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')

            query = f'''
            INSERT INTO `mai-database`.process_runs (Location, Process, Setup, Status, StartTime, dateID, updateStartDate, updateEndDate)
            VALUES (
                '{loc}',
                '{process}',
                '{setup}',
                'started',
                NOW(),
                '{dateID}',
                '{startDate}',
                '{endDate}'
            )
            '''
            
            cursor = cnx.cursor()
            cursor.execute(query)
            cnx.commit()
            
    except:
        logger.debug(f'ERROR: could not create a process for {loc}:')
        logger.debug(f'query: {query}')
        return None
    
    finally:
        # Close the cursor and connection in the finally block
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()


def updateUpdateProcess(loc, process, setup, dateID, column, value, replace = False):
        
    cnx = None
    cursor = None
    try:
        with db_lock:
    
            cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')

            query = (
                f'''UPDATE `mai-database`.process_runs 
                SET {column} = '{value}' 
                WHERE Location = '{loc}'
                AND Process = '{process}'
                AND Setup = '{setup}'
                AND dateID = '{dateID}';
                ''')
            
            if replace == False:
                if checkUpdateProcessStatus(loc, process, setup, dateID):
                    return
                
            cursor = cnx.cursor()        
            cursor.execute(query)
            cnx.commit()
            
    except mysql.connector.Error as error:
        print("Error updating")
        
    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()


def checkUpdateProcessStatus(loc, process, setup, dateID, column = 'Status'):
    cnx = None
    cursor = None
    try:
        with db_lock:
    
            cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')

            query = f'''
            SELECT {column} FROM `mai-database`.process_runs
            WHERE Location = '{loc}'
            AND Process = '{process}'
            AND Setup = '{setup}'
            AND dateID = '{dateID}'
            '''

            cursor = cnx.cursor()
            cursor.execute(query)
            response = cursor.fetchall()
            cursor.close()
            if response:
                return [row for row in response][0][0]
            else:
                return None
    
    except:
        logger.debug(f'Error: could not check a process for {loc}:')
        
    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()


def allProcessRecords(loc):
    engine = create_engine("mysql+mysqlconnector://root:BMkjM8_)-tN8R33u@34.72.234.161:3306/mai-database", pool_size=10, max_overflow=20)

    query = f'''
    SELECT * FROM `mai-database`.process_runs
    WHERE Location = '{loc}'
    ORDER BY StartTime desc;
    '''

    df = pd.read_sql(query, con=engine)
    return df
            
            
def getLocsForActivityUploadUpdate(name):

    cnx = mysql.connector.connect(user='root',password='BMkjM8_)-tN8R33u',host='34.72.234.161',database='mai-database')

    query = f'''
    SELECT l.Location, pr.dateID, pr.updateStartDate, pr.updateEndDate
    FROM `mai-database`.`process_runs` pr 
    LEFT JOIN `mai-database`.`location_file` l 
    ON pr.Location = l.Location
    WHERE pr.dateID is not null
    AND pr.Process = 'activityExportUpdate'
    AND pr.Status = 'complete'
    AND l.Assignment = '{name}'
    AND NOT EXISTS (
        SELECT 1 FROM `mai-database`.`process_runs` pr2
        WHERE pr2.Process = 'activityUploadUpdate'
        AND pr2.Location = l.Location
        AND (pr2.Status = 'complete' OR pr2.Status = 'failed')
        AND pr2.dateID = pr.dateID
    ) 
    AND EXISTS (
        SELECT 1 FROM activity_market a
        WHERE a.Location = pr.Location
    )
    AND NOT EXISTS (
        SELECT 1 FROM `mai-database`.`process_runs` pr3
        WHERE pr3.Process = 'activityExportUpdate'
        AND pr3.Location = pr.Location
        AND pr3.updateStartDate < pr.updateStartDate
        AND pr3.updateEndDate > pr.updateEndDate 
    )
    ORDER BY pr.updateStartDate ASC;
    '''

    cursor = cnx.cursor()
    cursor.execute(query)
    locs = cursor.fetchall()
    cursor.close()
    cnx.close()
    
    return locs


def assignForProcessing(loc, setupImg, setupMap, setupActivityPrep, setupActivity, nameList = ['paul', 'tillmann', 'eivind', 'sam', 'anna']):
    
    procLocCounts = {}
    for name in nameList:
        locsToProcess = getAssignedLocs(name, setupImg, setupMap, setupActivityPrep, setupActivity, status = 'for_process')
        locsForUpdateActPrep = getLocsToUpdate(name, status="for_activity_prep")
        locsForUpdateActExport = getLocsToUpdate(name, status="for_activity_export")
        locCount = len(locsToProcess + locsForUpdateActPrep + locsForUpdateActExport)
        procLocCounts[name] = locCount

    #get the smallest locCount
    minLocCount = min(procLocCounts.values())

    #get the name of the person with the smallest locCount
    minLocCountName = [name for name, locCount in procLocCounts.items() if locCount == minLocCount][0]

    #assign the loc to the person with the smallest locCount
    updateLocationFileStatus(loc, "Assignment", minLocCountName, replace=True)
    print(f'Assigning {loc} to {minLocCountName} for processing.')


def getLocsToUpdate(name, status="for_download"):
    cnx = mysql.connector.connect(
        user="root",
        password="BMkjM8_)-tN8R33u",
        host="34.72.234.161",
        database="mai-database",
    )
    cursor = cnx.cursor()

    if (
        status == "for_download"
    ):  # Changed by Tillmann to filter only locations with finished activity exports
        query = f"""
            SELECT `Location` FROM `mai-database`.location_file l
            WHERE `Assignment` = '{name}' 
            AND `for_updating` = 1
            AND DATEDIFF(CURDATE(), lastImageUpdate) > 30
            AND (NOT false_positive = 1 OR false_positive IS NULL)
            AND EXISTS (
                SELECT 1
                FROM `mai-database`.`process_runs` pr
                WHERE pr.Location = l.Location
                AND pr.Process = '04ActivityExport'
                AND pr.Setup = 'exportAct5'
                AND pr.Status = 'complete'
                )
                AND EXISTS (
                    SELECT 1
                    FROM `mai-database`.`activity_market` am
                    WHERE am.Location = l.Location
                    GROUP BY am.Location
                    HAVING COUNT(*) >= 2
                )
            """
        # Condition that there should be at least two entries controls for locations which have been marked as failed in the "activity_metric column" based on faulty outputs. These locations have only one entry.
        cursor.execute(query)
        response = cursor.fetchall()
        cursor.close()
        cnx.close()

        return [row[0] for row in response]

    if status == "for_activity_prep":
        query = f"""
            SELECT l.Location, pr.dateID
            FROM `mai-database`.`location_file` l 
            LEFT JOIN `mai-database`.`process_runs` pr
            ON l.Location = pr.Location
            WHERE pr.Process = 'imageDownload'
            AND l.`for_updating` = 1
            AND pr.Status = 'complete'
            AND l.Assignment = '{name}'
            AND NOT EXISTS (
                SELECT 1 FROM process_runs pr2
                WHERE pr2.Location = l.Location
                AND pr2.Process = 'activityPrepUpdate'
                AND pr2.dateID = pr.dateID
                AND (pr2.Status = 'complete' OR pr2.Status = 'failed')
                )
            AND EXISTS (
                SELECT 1
                FROM `mai-database`.`process_runs` pr
                WHERE pr.Location = l.Location
                AND pr.Process = '04ActivityExport'
                AND pr.Setup = 'exportAct5'
                AND pr.Status = 'complete'
                )
            """

    if status == "for_activity_export":
        query = f"""
            SELECT l.Location, pr.dateID
            FROM `mai-database`.`location_file` l 
            LEFT JOIN `mai-database`.`process_runs` pr
            ON l.Location = pr.Location
            WHERE pr.Process = 'activityPrepUpdate'
            AND l.`for_updating` = 1
            AND pr.Status = 'complete'
            AND l.Assignment = '{name}'
            AND NOT EXISTS (
                SELECT 1 FROM process_runs pr2
                WHERE pr2.Location = l.Location
                AND pr2.Process = 'activityExportUpdate'
                AND pr2.dateID = pr.dateID
                AND pr2.Status = 'complete'
                )
            """

    if status == "for_activity_upload":
        query = f"""
            SELECT l.Location, pr.dateID, pr.updateStartDate, pr.updateEndDate
            FROM `mai-database`.`location_file` l 
            LEFT JOIN `mai-database`.`process_runs` pr
            ON l.Location = pr.Location
            WHERE pr.Process = 'activityExportUpdate'
            AND l.`for_updating` = 1
            AND pr.Status = 'complete'
            AND l.Assignment = '{name}'
            AND NOT EXISTS (
                SELECT 1 FROM process_runs pr2
                WHERE pr2.Location = l.Location
                AND pr2.Process = 'activityUploadUpdate'
                AND pr2.dateID = pr.dateID
                AND pr2.Status = 'complete'
                )
            """

    if status == "for_prop_export":
        query = f"""
            SELECT l.Location, pr.dateID
            FROM `mai-database`.`location_file` l 
            RIGHT JOIN `mai-database`.`process_runs` pr
            ON l.Location = pr.Location
            WHERE pr.Process = 'imageDownload'
            AND l.`for_updating` = 1
            AND pr.Status = 'complete'
            AND l.Assignment = '{name}'
            AND NOT EXISTS (
                SELECT 1 FROM process_runs pr2
                WHERE pr2.Location = l.Location
                AND pr2.Process = 'propExport'
                AND pr2.dateID = pr.dateID
                AND pr2.Status = 'complete')
            """

    if status == "any":
        query = f"""
            SELECT Location FROM `mai-database`.location_file 
            WHERE Assignment = '{name}' 
            AND for_updating = 1
            AND DATEDIFF(CURDATE(), lastActivityUpdate) > 30"""

    cursor.execute(query)
    response = cursor.fetchall()
    cursor.close()
    cnx.close()

    return [row for row in response]

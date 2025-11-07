# Standard Library
import os
import re
import json
import time
import threading
import subprocess
import random
import logging
import pyproj
from datetime import datetime, timedelta

# Third-Party Libraries
import requests
import pandas as pd
import geopandas as gpd
import ee
import mysql.connector
import gcsfs
from google.cloud import storage
from dotenv import load_dotenv
from shapely.geometry import MultiPoint, mapping
from shapely.ops import transform

# Local Modules
from MAI2023.src.activityFunctions import *
from MAI2023.src.dbFunctions import *
from MAI2023.src.bucketFunctions import *
from MAI2023.main import *

client = storage.Client()
bucketName = "mai_2023"
bucket = client.get_bucket(bucketName)
order_url = "https://api.planet.com/compute/ops/orders/v2"
search_url = "https://api.planet.com/data/v1/quick-search"
with open("./MAI2023/cred.txt", "r") as f:
    cred = f.readlines()
cred = cred[0]
file_lock = threading.Lock()
masterLocationFile_lock = threading.Lock()
session = requests.Session()
maxCloudCover = 0.5
minClearPercent = 90
colspecs = [(0, 24), (26, 38), (40, 82), (84, 93), (95, 1000)]
max_retries = 10
retry_interval = 5
date_pattern1 = r"_20\d{2}-\d{2}-\d{2}_"
date_pattern2 = r"PSScene/20\d{2}\d{2}\d{2}_"
nodePath = str(subprocess.check_output(["which node"], shell=True))[2:-3]
eerunnerPath = str(subprocess.check_output(["which ee-runner"], shell=True))[2:-3]
logger = logging.getLogger("logging")
db_lock = threading.Lock()

def runMasterUpdate(
    name,
    private_key,
    debug=False,
    GEEproject=None,
    processes=["downloading","processing"], 
    maxThreads = 15
):
    # set up logger config
    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO)

    # authorize planet API session
    # set up API key:
    load_dotenv(
        dotenv_path=os.path.expanduser("~/.env")
        if os.geteuid() != 0
        else "/root/MAI2023/.env"
    )

    PLANET_API_KEY = os.environ.get("PLANET_API_KEY")
    print(f"using API key: {PLANET_API_KEY}")

    session = requests.Session()
    session.auth = (PLANET_API_KEY, "")

    if (not GEEproject and os.environ.get("GEE_PROJECT")):
        GEEproject = os.environ.get("GEE_PROJECT")

    # ensure that 'earthengine task list' will work properly
    print(f"using GEE project: {GEEproject}")
    subprocess.call(["earthengine", "set_project", GEEproject])

    # initialize earth engine api
    ee.Initialize()

    # create threads for each function and pass variables as args
    downThread = threading.Thread(
        target=downloadThreaderUpdate, args=(name, private_key,maxThreads)
    )
    
    actPrepThread = threading.Thread(
        target=updateProcessorActivityPrep,  # changed "paul" in args to "name"
        args=(name, "03_activityUpdater_20240604", "June06maxpMax", GEEproject),
    )
    
    actExpThread = threading.Thread(
        target=updateProcessorActivityExport,
        args=(name, "04_activityUpdater_20240620", "June06maxpMax", "exportAct5", GEEproject),
    )
    
    propThread = threading.Thread(
        target=updateProcessorPropExport, kwargs={"name": name, 'GEEproject': GEEproject}
    )
    
    actThread = threading.Thread(target=activityUpdateRunner, args=(name,))

    # start the threads
    if "downloading" in processes:
        downThread.start()
    if "processing" in processes:
        actPrepThread.start()
        actExpThread.start()
        propThread.start()
        actThread.start()

    # join the threads to wait for their completion
    if "downloading" in processes:
        downThread.join()
    if "processing" in processes:
        actPrepThread.join()
        actExpThread.join()
        propThread.join()
        actThread.join()

    print(f"Whoop whoop: All done!")


# ---------------------------------------------------#
# Download Functions
# ---------------------------------------------------#


def downloadThreaderUpdate(name, private_key,maxThreads):
    print("Downloading started...")
    
    while True:

        # pull list of locs that need downloads
        locList = getLocsToUpdate(name)
        print(f"{len(locList)} new locs for download: {locList[0:5]}...")
        runningLocs = []

        # while there are still any locs that need downloads
        while locList:
            # while active threads < 5 and locs are in the list that aren't in an active thread
            while len(runningLocs) < maxThreads and [
                loc for loc in locList if loc not in runningLocs
            ]:
                # get the first loc in the list that isn't in an active thread
                loc = [loc for loc in locList if loc not in runningLocs][0]

                updateLocationFileStatus(loc, "00DownStatus", "updating", replace=True)

                startDate = (checkLocationFileStatus(loc, "lastImageUpdate").strftime("%Y-%m-%d") + "T00:00:00Z")
                endDate = datetime.today().strftime("%Y-%m-%d")
                dateID = "_" + endDate.replace("-", "")

                startUpdateProcess(loc, "imageDownload", "", dateID, startDate[0:10], endDate)

                # start a new download thread with the loc
                thread = threading.Thread(
                    target=downloaderUpdate,
                    args=(
                        name,
                        loc,
                        runningLocs,
                        startDate,
                        endDate,
                        dateID,
                        private_key,
                        maxThreads,
                    ),
                )
                thread.start()
                # print(f'Started download thread for {loc}')

                # add that location to the list of locs with an active thread
                runningLocs.append(loc)
                print(f"Currently running download threads: {runningLocs}")

                time.sleep(15)
                # update the list of locs that need downloads
                locList = getLocsToUpdate(name)

            time.sleep(30)
        
        time.sleep(600)


def downloaderUpdate(
    name, loc, runningLocs, startDate, endDate, dateID, private_key, maxThreads
):
    # loop and request downloads

    # get current location status
    locGroup = checkLocationFileStatus(loc, "locGroup")
    bucket = checkLocationFileStatus(loc, "bucket")

    # set to 0 downloads initiated
    updateLocationFileStatus(loc, "00bDownloadsInitiated", 0, replace=True)

    # while loc downloads not completed or failed
    while loc in getLocsToUpdate(name):

        # find locs whose imagery downloads are processing and check if they have finished:
        if checkLocationFileStatus(loc, "00DownStatus") == "updating":
            checkExistingImagesUpdater(loc, locGroup, startDate, endDate, dateID)

            # check if the location has reached the attempted downloads limit -- if so, mark as failed
            if checkLocationFileStatus(loc, "00bDownloadsInitiated") > 4:
                print(f"{loc} reached download limit without success -- marking as failed.")
                updateLocationFileStatus(loc, "00DownStatus", "failed", replace=True)
                display(locationFileSummary(loc))

        if checkLocationFileStatus(loc, "stored_in_gcs")==0:
            remaining_assets = get_project_quotas([bucket])['freeAssetCount'][0]
            if remaining_assets<3000:
                print(f"Less than 3000 ({remaining_assets}) assets free in {bucket}. Remove {loc} from updating pipeline for now.")
                updateUpdateProcess(loc, "imageDownload", "", dateID, "Status", "failed", replace=True)
                updateUpdateProcess(loc, "imageDownload", "", dateID, "FailReason", "bucket full", replace=True)
                updateLocationFileStatus(loc, "00DownStatus", "paused_for_updates", replace=True)
                updateLocationFileStatus(loc, "for_updating", -1, replace=True)
        
        # if no orders are currently running for this location
        #if loc not in checkRunningOrders(update_orders = True) and loc in getLocsToUpdate(name): changed by Tillmann 2025-08-13
        if loc not in checkRunningOrders(False) and loc in getLocsToUpdate(name): 
            requestDownloadsUpdater(
                loc, locGroup, startDate, endDate, dateID, private_key, maxThreads
            )

        time.sleep(5)

    print(f"All downloads complete for {loc}!")
    runningLocs.remove(loc)
    logger.debug(f"Currently running download threads: {runningLocs}")


def requestDownloadsUpdater(
    loc, locGroup, startDate, endDate, dateID, private_key, maxThreads
):
    # Function to request the downloads needed for a given location.
    # Inputs:
    # new_products [list]:      list of sr-corrected image product ids that should be downloaded from Planet
    # forAnchoring [string]:    planet image product id that should be used for anchoring the downloaded images
    # private_key [string]:     file path to planet encrypted private key

    PLANET_API_KEY = os.environ.get("PLANET_API_KEY")
    session = requests.Session()
    session.auth = (PLANET_API_KEY, "")

    new_products, forAnchoring = checkExistingImagesUpdater(
        loc, locGroup, startDate, endDate, dateID
    )
    GEEbucket = checkLocationFileStatus(loc, "bucket")
    logger.debug(f"New products found: {new_products[0:5]} ...")
    orders = []

    geojson_data = get_buffered_market_shape(loc)
    lenCurrRunTasks = 100
    #currRunnTasks = list(checkRunningOrders(update_orders = True))
    currRunnTasks = list(checkRunningOrders(False)) # Changed by TIllmann 2025-08-13
    lenCurrRunTasks = len(currRunnTasks)

    if lenCurrRunTasks > maxThreads:
        logger.debug(
            f"{lenCurrRunTasks} running orders -- waiting for some to finish before starting {loc}..."
        )
        time.sleep(120)
    else:
        max_retries = 10

        # add to the number of times downloads have been attempted
        downloadsAttempted = checkLocationFileStatus(loc, "00bDownloadsInitiated")
        updateLocationFileStatus(
            loc, "00bDownloadsInitiated", downloadsAttempted + 1, replace=True
        )

        if len(new_products) > 10:
            print(
                f"SR download initiated for {loc} -- requesting {len(new_products)} products"
            )
            updateLocationFileStatus(loc, "00DownStatus", "initiated", replace=True)
            stored_in_gcs = checkLocationFileStatus(loc, "stored_in_gcs")
            for i in range(0, len(new_products), 499):
                logger.debug(f"Chunk {i}")
                itemIDs = []
                itemIDs.extend(new_products[i : i + 499])
                itemIDs.append(str(forAnchoring))

                order_payload=False
                if checkLocationFileStatus(loc, "stored_in_gcs") == 0:

                    bucketFolder = ee.data.listAssets(
                        f"projects/{GEEbucket}/assets/PS_imgs"
                    )["assets"][0]["id"].split("/")[-1]
                    
                    order_payload = fn_order_payload_ee()
                    order_payload["products"][0]["item_ids"] = itemIDs
                    order_payload["name"] = f'{loc} update chunk {i}'
                    order_payload["delivery"]["google_earth_engine"]["collection"] = f'PS_imgs/{bucketFolder}/{loc}'
                    order_payload["delivery"]["google_earth_engine"]["project"] = GEEbucket
                    order_payload["delivery"]["google_earth_engine"]["credentials"] = private_key
                    order_payload["tools"][0]["clip"]['aoi'] = geojson_data["geometry"]
                    #order_payload["tools"][2]["coregister"]['anchor_item'] = forAnchoring

                if checkLocationFileStatus(loc, "stored_in_gcs") == 1:
                
                    order_payload = fn_order_payload()
                    order_payload["products"][0]["item_ids"] = itemIDs
                    order_payload["name"] = f"{loc} update chunk {i}"
                    order_payload["delivery"]["google_cloud_storage"]["bucket"] = (f"ps-imgs-mai1")
                    order_payload["delivery"]["google_cloud_storage"]["path_prefix"] = loc
                    order_payload["delivery"]["google_cloud_storage"]["credentials"] = (private_key)
                    order_payload["tools"][0]["clip"]["aoi"] = geojson_data["geometry"]
                    #order_payload["tools"][3]["coregister"]["anchor_item"] = forAnchoring

                # print('order_payload',order_payload)
                if order_payload:
                    for attempt in range(max_retries + 1):
                        try:
                            order_response = session.post(order_url, json=order_payload)
                            if order_response.status_code == 202:
                                # Request succeeded: add to number of downloads initiated and break out of the loop
                                break
                            else:
                                logger.debug(
                                    f"Request attempt {attempt + 1} failed with status code: {order_response.status_code}"
                                )
                        except Exception as e:
                            logger.debug(
                                f"Request attempt {attempt + 1} failed with error: {e}"
                            )
                        if attempt < max_retries:
                            # Sleep before the next retry
                            time.sleep(retry_interval)
                        else:
                            logger.debug("Maximum retry attempts reached. Request failed.")
                orders.append(order_response.json())

                logger.debug(f"Order status code: {order_response.status_code}")
                try:
                    logger.debug(f"Order ID: {order_response.json()['id']} \n")
                except:
                    logger.debug("failed")

                if order_response.status_code != 202:
                    logger.debug(order_response.json())
                    if (
                        "Order request resulted in no acceptable assets"
                        in json.dumps(order_response.json())
                        or "Unable to accept order: Cannot coregister single item. "
                        in json.dumps(order_response.json())
                    ):
                        logger.debug(
                            f"SR download marked complete for {loc} since no new items were registered."
                        )
                        updateLocationFileStatus(
                            loc, "00DownStatus", "complete", replace=True
                        )
                        updateLocationFileStatus(
                            loc, "lastImageUpdate", endDate, replace=True
                        )
                        updateUpdateProcess(
                            loc,
                            "imageDownload",
                            "",
                            dateID,
                            "Status",
                            "complete",
                            replace=True,
                        )
                # time.sleep(600) #1200

                # print(f'SR download initiated for {loc}')
                updateLocationFileStatus(loc, "00DownStatus", "updating", replace=True)
                time.sleep(60)
        else:
            updateLocationFileStatus(loc, "00DownStatus", "complete", replace=True)
            startDate = (
                checkLocationFileStatus(loc, "lastImageUpdate").strftime("%Y-%m-%d")
                + "T00:00:00Z"
            )
            endDate = datetime.today().strftime("%Y-%m-%d")
            dateID = "_" + endDate.replace("-", "")
            startUpdateProcess(
                loc, "imageDownload", "", dateID, startDate[0:10], endDate
            )
            updateUpdateProcess(
                loc, "imageDownload", "", dateID, "Status", "complete", replace=True
            )
            updateLocationFileStatus(loc, "lastImageUpdate", endDate, replace=True)
            print(f"no new downloads for {loc}")


def checkExistingImagesUpdater(loc, locGroup, startDate, endDate, dateID):
    # Takes a location, locGroup, and end date, and compares available Planet imagery to what is currently downloaded.
    # If this is the first time running for this loc, it creates a storage folder for the downloaded images.
    #
    # Inputs:
    # loc:      string (e.g: "lon14_115lat38_4743")
    # locGroup: string (e.g: "79_Tigray_1")
    # endDate:  string (e.g: "2023-09-30")
    #
    # Returns:
    # new_products:      list of planet product IDs for SR images that we want, but don't have.
    # forAnchoring:      string of a planet product ID to use for anchoring (QUESTION: could re-making the anchor each time cause problems?
    # GEEbucket:         string of a generated GEE image bucket name for this location

    PLANET_API_KEY = os.environ.get("PLANET_API_KEY")
    session = requests.Session()
    session.auth = (PLANET_API_KEY, "")

    saveConvexHull(loc)  # Added by Tillmann

    logger.debug(
        f"Checking existing images for {loc} in {locGroup} from {startDate} to {endDate}:"
    )

    # If image downloads already running, don't run.
    #locsRunningOrders = checkRunningOrders(update_orders = True) changed by Tillmann 2025-08-13
    locsRunningOrders = checkRunningOrders(False)
    logger.debug(f"locs with running Planet orders: {set(locsRunningOrders)}")
    if loc in locsRunningOrders:
        logger.debug(f"Downloads currently running for {loc} -- skipping image check.")
        new_products, forAnchoring = ("none", "none")
        time.sleep(180)

    else:
        GEEbucket = checkLocationFileStatus(loc, "bucket")

        # search for collections that already exist for the location, and store them in a list
        logger.debug(f"Looking up existing imagery for {loc}...")
        pattern = r"{}/(.*?)_3B_AnalyticMS".format(loc)  # pattern to search for
        tif_pattern = r"\.tif$"  # Matches strings ending with .tif
        latestDate = "2016-01-01"
        existing = []
        name = f"{locGroup}/loc{loc}/"
        if checkLocationFileStatus(loc, "stored_in_gcs") == 0:
            bucketFolder = ee.data.listAssets(f"projects/{GEEbucket}/assets/PS_imgs")[
                "assets"
            ][0]["id"].split("/")[-1]
            os.system(
                f'earthengine ls "projects/{GEEbucket}/assets/PS_imgs/{bucketFolder}/{loc}"> ./temp/alreadyUp{loc}.txt'
            )
            with open(f"./temp/alreadyUp{loc}.txt", "r") as file:
                for line in file:
                    match = re.search(pattern, line.strip())
                    if match:
                        existing.append(match.group(1))

        if checkLocationFileStatus(loc, "stored_in_gcs") == 1:
            os.system(
                f"gcloud storage ls --recursive gs://ps-imgs-mai1/{loc} > ./temp/alreadyUp{loc}.txt"
            )
            with open(f"./temp/alreadyUp{loc}.txt", "r") as file:
                for line in file:
                    line = line.strip()
                    if re.search(pattern, line):
                        tif_match = re.search(tif_pattern, line)
                        if tif_match:
                            existing.append(line)  # Append the entire

        if checkLocationFileStatus(loc, "stored_in_gcs") == 0:
            logger.debug(
                f"Found {len(existing)} images for {loc} -- updating location file."
            )
            updateLocationFileStatus(
                loc, "totalDownloaded", len(existing), replace=True
            )
        if checkLocationFileStatus(loc, "stored_in_gcs") == 1:
            logger.debug(
                f"Found {len(existing) / 2} images for {loc} -- updating location file."
            )
            updateLocationFileStatus(
                loc, "totalDownloaded", len(existing) / 2, replace=True
            )

        geojson_data = get_buffered_market_shape(loc)

        ### Look for an anchor image
        # Search parameters for anchoring image
        search_percent = 99
        while True:  # loop until anchor image is found
            search_para_1 = fn_search_para_1()
            search_para_1["filter"]["config"][0]["config"]["coordinates"] = (
                geojson_data["geometry"]["coordinates"]
            )
            search_para_1["filter"]["config"][1]["config"]["gte"] = (
                "2020-01-01" + "T00:00:00Z"
            )
            search_para_1["filter"]["config"][1]["config"]["lte"] = (
                endDate + "T23:59:59Z"
            )
            search_para_1["filter"]["config"][2]["config"]["lte"] = 0  # cloud cover
            search_para_1["filter"]["config"][3]["config"]["lte"] = (
                0  # anomalous_pixels
            )
            search_para_1["filter"]["config"][4]["config"]["gte"] = (
                search_percent  # clear_confidence_percent
            )
            search_para_1["filter"]["config"][5]["config"]["gte"] = (
                search_percent  # clear_percent
            )
            search_para_1["filter"]["config"][6]["config"] = ["true"]  # ground_control
            # print('search_para_1',search_para_1)

            search_percent -= 1  # reduce by 1 before next loop

            # Search for anchor products using the Data API
            max_retries = 10
            for attempt in range(max_retries + 1):
                try:
                    search_response = session.post(search_url, json=search_para_1)
                    if search_response.status_code == 200:
                        # Request succeeded, break out of the loop
                        break
                    else:
                        logger.debug(
                            f"Request attempt {attempt + 1} failed with status code: {search_response.status_code}"
                        )
                except Exception as e:
                    logger.debug(
                        f"Request attempt {attempt + 1} failed with error: {e}"
                    )
                if attempt < max_retries:
                    # Sleep before the next retry
                    time.sleep(retry_interval)
                else:
                    logger.debug("Maximum retry attempts reached. Request failed.")

            # print('Search status code:',search_response.status_code)
            # if search_response.status_code != 200:
            #    print(search_response.json())
            #    stop
            # Count number of features in first page
            geojson = search_response.json()
            features = geojson["features"]

            # Loop over all pages to count total number of features
            while True:
                next_link = geojson.get("_links", {}).get("_next")
                if next_link is None:
                    break

                page_url = next_link
                for attempt in range(max_retries + 1):
                    try:
                        r = session.get(page_url)
                        if r.status_code == 200:
                            # Request succeeded, break out of the loop
                            break
                        else:
                            logger.debug(
                                f"Request attempt {attempt + 1} failed with status code: {r.status_code}"
                            )
                    except Exception as e:
                        logger.debug(
                            f"Request attempt {attempt + 1} failed with error: {e}"
                        )
                    if attempt < max_retries:
                        # Sleep before the next retry
                        time.sleep(retry_interval)
                    else:
                        logger.debug("Maximum retry attempts reached. Request failed.")
                geojson = r.json()
                features += geojson["features"]

            if len(features) != 0:
                logger.debug(
                    f"Image found for anchoring with search_percent = {search_percent}"
                )
                break  # break out of loop once anchor image is found

        # Retrieve the product IDs from the search response
        product_ids = []
        for i in features:
            product_ids.append(i["id"])  # EDIT:

        # Arbitrarily choose the last image for anchoring
        forAnchoring = product_ids[-1]

        # Get images to download
        features_sr = searchAvailableImgsUpdater(
            geojson_data, startDate, endDate, maxCloudCover
        )

        # Retrieve the product IDs from the search response that we don't already have
        product_ids = []
        for i in features_sr:
            product_ids.append(i["id"])
        new_products = remove_overlapping_strings(product_ids, existing)
        # print(f'total available SR products found for {loc}:', product_ids)
        # print(f'existing SR products found for {loc}:', existing)
        # print(f'new SR products deduced:', new_products)

        logger.debug(f"{loc} total number of products available: {len(product_ids)}")
        logger.debug(f"{loc} total number of existing products: {len(existing)}")
        logger.debug(f"{loc} number of new products available: {len(new_products)}")

        # If the number of new products is less than 5 for both download types, mark the location complete
        if len(new_products) < 6:

            # delete duplicate images
            if checkLocationFileStatus(loc, "stored_in_gcs") == 1:
                deleteDuplicates_gcs(loc)
                # prepare_imagery_gcs(loc)
                # Extract harmonized files and image IDs
                _, image_IDs = extract_harmonized_files_and_ids(
                    f"gs://ps-imgs-mai1/{loc}", loc
                )
                logger.debug("image_IDs: %d, %s", len(image_IDs), image_IDs[0:5])

                # Process JSON files and create FeatureCollection
                features_json, features = process_json_files(loc)
                geojson = create_geojson(features_json)
                client = storage.Client()
                bucket = client.bucket("ps-imgs-mai1")
                blob = bucket.blob(f"imgProperties/{loc}.geojson")
                blob.upload_from_string(
                    json.dumps(geojson), content_type="application/json"
                )
                print("image properties uploaded", loc)
                #tryPropertiesExport(locGroup, loc, GEEbucket, dateID, GEEproject)

            if checkLocationFileStatus(loc, "stored_in_gcs") == 0:
                deleteDuplicates(loc)

            assignForProcessing(
                loc,
                setupImg = 'Apr24', setupMap = 'MpM6', setupActivityPrep = 'June06maxpMax', setupActivity = 'exportAct5',nameList = ['paul','tillmann', 'sam','eivind']
            )
            updateLocationFileStatus(loc, "00DownStatus", "complete", replace=True)
            updateLocationFileStatus(loc, "lastImageUpdate", endDate, replace=True)
            updateUpdateProcess(
                loc, "imageDownload", "", dateID, "Status", "complete", replace=True
            )
            
            print(f"Downloading complete for {loc}!")

    return new_products, forAnchoring


def searchAvailableImgsUpdater(geojson_data, startDate, endDate, maxCloudCover):
    PLANET_API_KEY = os.environ.get("PLANET_API_KEY")
    session = requests.Session()
    session.auth = (PLANET_API_KEY, "")

    logger.debug(f"Requesting available images for cloud cover {maxCloudCover}...")

    # Create new search parameters to capture all images of interest
    search_para_2 = fn_search_para_2()
    search_para_2["filter"]["config"][0]["config"]["coordinates"] = geojson_data[
        "geometry"
    ]["coordinates"]
    search_para_2["filter"]["config"][1]["config"]["gte"] = startDate
    search_para_2["filter"]["config"][1]["config"]["lte"] = endDate + "T23:59:59Z"
    search_para_2["filter"]["config"][2]["config"]["lte"] = maxCloudCover

    # Search for products using the Data API
    for attempt in range(max_retries + 1):
        try:
            search_response = session.post(search_url, json=search_para_2)
            if search_response.status_code == 200:
                # Request succeeded, break out of the loop
                break
            else:
                logger.debug(
                    f"Request attempt {attempt + 1} failed with status code: {search_response.status_code}"
                )
        except Exception as e:
            logger.debug(f"Request attempt {attempt + 1} failed with error: {e}")
        if attempt < max_retries:
            # Sleep before the next retry
            time.sleep(retry_interval)
        else:
            logger.debug("Maximum retry attempts reached. Request failed.")

    # Retrieve all features that are returned from the seatch
    geojson = search_response.json()
    features = geojson["features"]
    # print('geojson ', loc ,geojson)
    # Loop over all pages to count total number of features
    while True:
        next_link = geojson.get("_links", {}).get("_next")
        if next_link is None:
            break

        page_url = next_link
        for attempt in range(max_retries + 1):
            try:
                r = session.get(page_url)
                if r.status_code == 200:
                    # Request succeeded, break out of the loop
                    break
                else:
                    logger.debug(
                        f"Request attempt {attempt + 1} failed with status code: {r.status_code}"
                    )
            except Exception as e:
                logger.debug(f"Request attempt {attempt + 1} failed with error: {e}")
            if attempt < max_retries:
                # Sleep before the next retry
                time.sleep(retry_interval)
            else:
                logger.debug("Maximum retry attempts reached. Request failed.")
        geojson = r.json()
        features += geojson["features"]

    # Create lists of the SR and non-SR features that are returned from the search
    features_sr = [
        feature
        for feature in features
        if "ortho_analytic_4b_sr" in feature["assets"]
        and "ortho_udm2" in feature["assets"]
    ]

    return features_sr


def fn_order_payload_ee():
     order_payload = {
         "name": None,#
         "order_type": "partial", # deliver only those items for which all parts of bundle are available
         "products": [{
             "item_ids": None, # to be filled in later
             "item_type": 'PSScene',
             "product_bundle": "analytic_sr_udm2"  # https://developers.planet.com/apis/orders/product-bundles-reference/
         }],
         "tools": [    # add or remove tools as needed
             {
                 "clip": {
                     "aoi": None #
                 }
             },
             {
                 "harmonize": {
                     'target_sensor': 'Sentinel-2'
                 }
             },
             #{
             #    "coregister": {
             #        "anchor_item": None # find the perfect image here (recent, little haze, little distortion)
             #    }
             #}
         ],
         "delivery": {
         "google_earth_engine": {
             "project": None,
             "collection": None,
             "credentials": None
         }
         }
     }
     return order_payload


def get_buffered_market_shape(loc, buffer_meters=20, simplify_tolerance=5.0):
    """
    Compute the buffered and simplified convex hull of input market shapes.

    Parameters:
    - mkt_shape_json (dict): The JSON object containing market shape geometry data.
    - buffer_meters (float): Distance in meters to buffer the convex hull.
    - simplify_tolerance (float): Simplification tolerance in meters.

    Returns:
    - dict: A GeoJSON Feature with a simplified, buffered convex hull polygon.
    """
    
    mkt_shape_json = json.loads(checkLocationFileStatus(loc, 'mktShape'))
    
    all_coords = []
    
    # Collect all coordinates from the features
    for weekday_data in mkt_shape_json.values():
        for feature in weekday_data['features']:
            geom = shape(feature['geometry'])
            if isinstance(geom, Polygon):
                all_coords.extend(list(geom.exterior.coords))
            elif isinstance(geom, MultiPolygon):
                for poly in geom.geoms:
                    all_coords.extend(list(poly.exterior.coords))
    
    # Return None if no geometry found
    if not all_coords:
        return None

    # Compute convex hull
    points = MultiPoint(all_coords)
    hull = points.convex_hull

    # Setup projection to/from UTM for accurate buffering
    wgs84 = pyproj.CRS("EPSG:4326")
    lon = all_coords[0][0]
    utm_zone = int((lon + 180) / 6) + 1
    is_southern = all_coords[0][1] < 0
    utm_code = f"EPSG:{32700 + utm_zone if is_southern else 32600 + utm_zone}"
    utm = pyproj.CRS(utm_code)

    project_to_utm = pyproj.Transformer.from_crs(wgs84, utm, always_xy=True).transform
    project_to_wgs = pyproj.Transformer.from_crs(utm, wgs84, always_xy=True).transform

    # Buffer and simplify
    hull_utm = transform(project_to_utm, hull)
    buffered = hull_utm.buffer(buffer_meters)
    simplified = buffered.simplify(simplify_tolerance, preserve_topology=True)
    simplified_wgs = transform(project_to_wgs, simplified)

    # Return as GeoJSON Feature
    return {
        "type": "Feature",
        "geometry": mapping(simplified_wgs),
        "properties": {}
    }


# ---------------------------------------------------#
# Processing Functions
# ---------------------------------------------------#


def updateProcessorPropExport(name, GEEproject):
    # loops through a given list of locations within a locGroup to run checks and, if necessary, processing tasks
    # continues to loop continuously until all tasks have been concluded for all locations in the list

    print("Started prop export processor...")

    while True:
        locs = getLocsToUpdate(name, status="for_prop_export")
        if locs:
            # while there are any assigned locations not yet fully processed
            while locs:
                # get a list of locations that have completed downloading, but not finished processing
                print(f"{len(locs)} locs for prop export: {locs[0:5]}...")
                # if there are any locations ready to process
                for loc, dateID in locs:
                    logger.debug(f"Beginning processing loop for {loc}...")
                    locGroup = checkLocationFileStatus(loc, "locGroup")
                    GEEbucket = checkLocationFileStatus(loc, "bucket")

                    # try processing step
                    tryPropertiesExport(locGroup, loc, GEEbucket, dateID, GEEproject)
                    time.sleep(10)

                # wait in between loops through all the locations
                time.sleep(300)

                # re-update the locations for processing
                locs = getLocsToUpdate(name, status="for_activity_prep")

        time.sleep(300)


def updateProcessorActivityPrep(name, activityPrepCodeFile, setupActivityPrep, GEEproject):
    # loops through a given list of locations within a locGroup to run checks and, if necessary, processing tasks
    # continues to loop continuously until all tasks have been concluded for all locations in the list

    print("Activity prep processing started...")

    # while there are any assigned locations not yet fully processed
    while True:
        locs = getLocsToUpdate(name, status="for_activity_prep")
        if locs:
            # while there are any assigned locations not yet fully processed
            while locs:
                # get a list of locations that have completed downloading, but not finished processing
                print(f"{len(locs)} locs for activity prep: {locs[0:5]}...")
                # if there are any locations ready to process
                for loc, dateID in locs:
                    # try:
                    logger.debug(f"Beginning processing loop for {loc}...")
                    locGroup = checkLocationFileStatus(loc, "locGroup")
                    country = checkLocationFileStatus(loc, "country")
                    GEEbucket = checkLocationFileStatus(loc, "bucket")

                    completedTasks = checkCompletedTasksGEE(loc)

                    if completedTasks:
                        lastCompletedTask = checkCompletedTasksGEE(loc)[0]

                        # check errors and update database accordingly:
                        if all(
                            term in lastCompletedTask
                            for term in ["actPrep_", "Image has no bands"]
                        ):
                            print(
                                f"03 Activity prep update for {loc} in {locGroup} failed due to empty ICs -- updating location file"
                            )
                            updateLocationFileStatus(
                                loc, "Assignment", "tillmann-updfail", replace=True
                            )

                            # updateUpdateProcess(loc, 'activityExportUpdate', setupActivityPrep, dateID, 'Status', 'failed')
                            # updateUpdateProcess(loc, 'activityPrepUpdate', setupActivityPrep, dateID, 'Status', 'failed')
                            print(
                                f"03 Activity prep update for {loc} in {locGroup} failed due to empty ICs -- updated location file"
                            )
                            continue

                    # try each processing step
                    tryActivityPrepUpdate(
                        locGroup,
                        loc,
                        dateID,
                        country,
                        GEEbucket,
                        activityPrepCodeFile,
                        setupActivityPrep,
                        GEEproject
                    )
                    time.sleep(2)
                    # except Exception as e:
                    #    print(f"Problem with {loc}",e)
                    #    pass

                # wait in between loops through all the locations
                time.sleep(300)

                # re-update the locations for processing
                locs = getLocsToUpdate(name, status="for_activity_prep")

        time.sleep(300)


def updateProcessorActivityExport(
    name, activityExportCodeFile, setupActivityPrep, setupActivity, GEEproject
):
    # loops through a given list of locations within a locGroup to run checks and, if necessary, processing tasks
    # continues to loop continuously until all tasks have been concluded for all locations in the list

    print("Activity export processing started...")
    while True:
        locs = getLocsToUpdate(name, status="for_activity_export")
        if locs:
            # while there are any assigned locations not yet fully processed
            while locs:
                # get a list of locations that have completed downloading, but not finished processing
                print(f"{len(locs)} locs for activity export: {locs[0:5]}...")
                # if there are any locations ready to process
                for loc, dateID in locs:
                    logger.debug(f"Beginning processing loop for {loc}...")
                    locGroup = checkLocationFileStatus(loc, "locGroup")
                    country = checkLocationFileStatus(loc, "country")
                    GEEbucket = checkLocationFileStatus(loc, "bucket")

                    completedTasks = checkCompletedTasksGEE(loc)

                    if completedTasks:
                        lastCompletedTask = checkCompletedTasksGEE(loc)[0]

                        # check errors and update database accordingly:
                        if all(
                            term in lastCompletedTask
                            for term in [setupActivity, "out of memory"]
                        ):
                            print(
                                f"04 Activity export update for {loc} in {locGroup} failed due to memory limits -- updating location file"
                            )
                            updateUpdateProcess(
                                loc,
                                "activityExportUpdate",
                                setupActivity,
                                dateID,
                                "Status",
                                "failed",
                                fail_reason="out of memory",
                            )
                            continue

                    # try each processing step
                    tryActivityExportUpdate(
                        locGroup,
                        loc,
                        dateID,
                        country,
                        GEEbucket,
                        activityExportCodeFile,
                        setupActivity,
                        setupActivityPrep,
                        GEEproject
                    )
                    time.sleep(2)

                # wait in between loops through all the locations
                time.sleep(300)

                # re-update the locations for processing
                locs = getLocsToUpdate(name, status="for_activity_prep")

        time.sleep(300)


def tryPropertiesExport(locGroup, loc, GEEbucket, dateID, GEEproject):
    # runs logical checks to see if properties export needs to be run for a given loc and locGroup,
    # runs the export if necessary, and updates the location file accordingly.

    # initialize google storage client
    client = storage.Client()

    # check if task is already running -- if so, exit and update
    if f"prop_{loc}" in str(
        subprocess.check_output(["earthengine task list --status RUNNING"], shell=True)
    ) or f"prop_{loc}" in str(
        subprocess.check_output(["earthengine task list --status READY"], shell=True)
    ):
        logger.debug(
            f"Properties export currently running for {loc} in {locGroup} -- updating location file."
        )
        return

    # set up and modify GEE code to prepare for running
    codeFile = f"./temp/_exportProp_{loc}.js"  # delete after
    if checkLocationFileStatus(loc, "stored_in_gcs") == 0:
        with open("./MAI/paul/imagePropertiesExporter", "r") as fin:
            with open(codeFile, "w") as fout:
                for line in fin:
                    fout.write(
                        line.replace("INSERT_LOC_GROUP_HERE", locGroup)
                        .replace('"INSERT_LOC_HERE"', loc)
                        .replace("INSERT_BUCKET_HERE", GEEbucket)
                    )
        subprocess.call([nodePath, "--no-deprecation", eerunnerPath, codeFile, f"--project={GEEproject}"])
        startDate = checkUpdateProcessStatus(
            loc, "imageDownload", "", dateID, "updateStartDate"
        ).strftime("%Y-%m-%d")
        endDate = checkUpdateProcessStatus(
            loc, "imageDownload", "", dateID, "updateEndDate"
        ).strftime("%Y-%m-%d")
        startUpdateProcess(loc, "propExport", "", dateID, startDate, endDate)
        updateUpdateProcess(
            loc, "propExport", "", dateID, "Status", "complete", replace=True
        )
        logger.debug(
            f"Properties download for {loc} in {locGroup} initiated  -- updating location file."
        )

    if checkLocationFileStatus(loc, "stored_in_gcs") == 1:
        fs = gcsfs.GCSFileSystem()
        with fs.open(f"gs://ps-imgs-mai1/imgProperties/{loc}.geojson") as f:
            geojson = gpd.read_file(f)
        df = pd.DataFrame(geojson.drop(columns="geometry"))
        with fs.open(
            f"gs://ps-imgs-mai1/{locGroup}/properties/propEx_{locGroup}_{loc}.csv", "w"
        ) as f:
            df.to_csv(f, index=False)
        startDate = checkUpdateProcessStatus(
            loc, "imageDownload", "", dateID, "updateStartDate"
        ).strftime("%Y-%m-%d")
        endDate = checkUpdateProcessStatus(
            loc, "imageDownload", "", dateID, "updateEndDate"
        ).strftime("%Y-%m-%d")
        startUpdateProcess(loc, "propExport", "", dateID, startDate, endDate)
        updateUpdateProcess(
            loc, "propExport", "", dateID, "Status", "complete", replace=True
        )
    print(f"properties updated for {loc}, {dateID}")


def tryActivityPrepUpdate(
    locGroup, loc, dateID, country, GEEbucket, activityPrepCodeFile, setup, GEEproject
):
    # runs logical checks to see if market activity export needs to be run for a given loc and locGroup,
    # runs the export if necessary, and updates the location file accordingly.

    # startDate = checkUpdateProcessStatus(loc, 'imageDownload', '', dateID, 'updateStartDate')
    # print(loc)
    datetime_str = checkLocationFileStatus(loc, "lastActivityUpdate")

    # Detect and parse the datetime string
    if len(datetime_str) == 10:  # "YYYY-MM-DD"
        date_format = "%Y-%m-%d"
    elif len(datetime_str) == 19:  # "YYYY-MM-DD HH:mm:ss"
        date_format = "%Y-%m-%d %H:%M:%S"
    else:
        raise ValueError(f"Unexpected datetime format: {datetime_str}")

    # Parse the date part
    startDate = datetime.strptime(datetime_str, date_format)

    # startDate = datetime.strptime(checkLocationFileStatus(loc, 'lastActivityUpdate'), "%Y-%m-%d")
    startDate = (startDate - timedelta(days=84)).strftime(
        "%Y-%m-%d"
    )  # subtract 84 days to include images who can achieve more complete composites
    endDate = checkUpdateProcessStatus(
        loc, "imageDownload", "", dateID, "updateEndDate"
    ).strftime("%Y-%m-%d")

    # initialize google storage client
    client = storage.Client()

    # if marked complete or failed, exit
    if checkUpdateProcessStatus(loc, "activityPrepUpdate", setup, dateID) in [
        "complete",
        "failed",
    ]:
        logger.debug(
            f"03 Activity prep already concluded for {loc} in {locGroup} -- skipping."
        )
        return

    # check if output already exists in GEE -- if so, exit and update
    bucketFolder = ee.data.listAssets(f"projects/{GEEbucket}/assets/PS_imgs")["assets"][
        0
    ]["id"].split("/")[-1]
    commandString = (
        f'earthengine ls "projects/{GEEbucket}/assets/PS_imgs/{bucketFolder}/{loc}proc"'
    )

    output = str(subprocess.check_output([commandString], shell=True))
    if f"exp{setup}_{loc}{dateID}" in output:
        if not checkUpdateProcessStatus(
            loc, "activityPrepUpdate", setup, dateID, column="Status"
        ):
            print(
                f"Tried to complete a process record that doesn't exist: loc: {loc}, process: activityPrepUpdate, dateID: {dateID}. Creating new process record."
            )
            startUpdateProcess(
                loc, "activityPrepUpdate", setup, dateID, startDate, endDate
            )
        updateUpdateProcess(
            loc, "activityPrepUpdate", setup, dateID, "Status", "complete", replace=True
        )
        print(
            f"03 Activity prep update succeeded for {loc} in {locGroup} -- updating location file."
        )
        return

    # check if task is already running -- if so, exit
    running = str(
        subprocess.check_output(["earthengine task list --status RUNNING"], shell=True)
    )
    ready = str(
        subprocess.check_output(["earthengine task list --status READY"], shell=True)
    )

    if f"actPrep_{loc}" in running + ready:
        logger.debug(f"03 Activity prep currently running for {loc} in {locGroup}.")
        return

    # set up and modify GEE code to prepare for running
    if checkLocationFileStatus(loc, "stored_in_gcs") == 0:
        codeFile = f"./temp/_activityPrepUpdate_{loc}.js"

        with open(f"./MAI/latest/{activityPrepCodeFile}", "r") as fin:
            with open(codeFile, "w") as fout:
                for line in fin:
                    fout.write(
                        line.replace("INSERT_LOC_GROUP_HERE", locGroup)
                        .replace('"INSERT_LOC_HERE"', loc)
                        .replace("INSERT_BUCKET_HERE", GEEbucket)
                        .replace("INSERT_COUNTRY_HERE", country)
                        .replace("INSERT_START_DATE_HERE", startDate)
                        .replace("INSERT_DATE_ID_HERE", dateID)
                    )

    if checkLocationFileStatus(loc, "stored_in_gcs") == 1:
        # Extract harmonized files and image IDs
        harmonized_files, _ = extract_harmonized_files_and_ids(
            f"gs://ps-imgs-mai1/{loc}", loc
        )
        # restriction of year<2022 necessary for exports to work. Can run  update in subsequent step to get full activity series. Exclude 2016 here
        harmonized_files = [
            s
            for s in harmonized_files
            if any(s.split("/")[-1].startswith(str(year)) for year in range(2020, 2030))
            and not any(
                s.split("/")[-1].startswith(exclude)
                for exclude in ["201701", "201702", "201703", "201704"]
            )
        ]
        logger.debug(
            "harmonized_files: %d, %s", len(harmonized_files), harmonized_files[0:5]
        )

        #### FILTER BY WEEKDAY STEP
        # not sure if this should go after or before the prev. step
        try:
            bucketFolder = ee.data.listAssets(f"projects/{GEEbucket}/assets/PS_imgs")[
                "assets"
            ][0]["id"].split("/")[-1]
            geePath = f"projects/{GEEbucket}/assets/PS_imgs/{bucketFolder}/{loc}proc/shp_MpM6_{locGroup}{loc}"
            fc = ee.FeatureCollection(geePath).filter(ee.Filter.lt("strictnessRank", 25))
            weekday_list = list(set(fc.aggregate_array("weekdayShp").getInfo()))
        except Exception as e:
            if "not found" in str(e).lower():
                startUpdateProcess(loc, "activityPrepUpdate", setup, dateID, startDate, endDate)
                updateUpdateProcess(loc, "activityPrepUpdate", setup, dateID, "Status", "failed", replace=True)
                updateUpdateProcess(loc, "activityPrepUpdate", setup, dateID, "FailReason", "gee fc not found", replace=True)
                return
            else:
                raise
                
        def extract_weekday(file_path):
            date_string = file_path.split("/")[-1][0:8]
            date_obj = datetime.strptime(date_string, "%Y%m%d")
            return (
                date_obj.weekday() + 1
            ) % 7  # add 1 and mod 7 to get Sunday=0 instead of Monday=0

        def extract_date(file_path):
            date_string = file_path.split("/")[-1][0:8]
            return datetime.strptime(date_string, "%Y%m%d")

        cutoff = datetime.strptime("2020-01-01", "%Y-%m-%d")

        harmonized_files_md = [
            file for file in harmonized_files
            if extract_date(file) > cutoff and extract_weekday(file) in weekday_list
        ]
        harmonized_files_non_md = [
            file for file in harmonized_files
            if extract_date(file) > cutoff and extract_weekday(file) not in weekday_list
        ]

        # randomly sample non-market-days up to 2000
        #total_files = len(harmonized_files_md) + len(harmonized_files_non_md)
        
        #if total_files > 1900:
        #    # Keep all market day files, sample non-market day files to reach 2500 total
        #    max_non_md_files = 1900 - len(harmonized_files_md)
        #    if max_non_md_files > 0:
        #        harmonized_files_non_md = random.sample(harmonized_files_non_md, min(max_non_md_files, len(harmonized_files_non_md)))
        #    else:
        #        print("ERROR: No room for non-market day files")
        #        stop 
                
        #harmonized_files = harmonized_files_md + harmonized_files_non_md

        max_total_files = 1900  # Specify maximum total files
        max_per_category = max_total_files // 2  # Half for each category
        
        num_md_available = len(harmonized_files_md)
        num_non_md_available = len(harmonized_files_non_md)
        
        if num_md_available < max_per_category:
            # MD files are limiting factor - use all MD files and fill rest with non-MD
            num_md = num_md_available
            num_non_md = min(max_total_files - num_md, num_non_md_available)
            
            harmonized_files_md = harmonized_files_md  # Use all available
            harmonized_files_non_md = random.sample(harmonized_files_non_md, num_non_md)
            
            print(f"MD files limited for {loc}: Using all {num_md} MD files and {num_non_md} non-MD files")
        else:
            # Enough MD files available - aim for equal split
            num_files_per_category = min(max_per_category, num_non_md_available)
            
            harmonized_files_md = random.sample(harmonized_files_md, num_files_per_category)
            harmonized_files_non_md = random.sample(harmonized_files_non_md, num_files_per_category)
            
            print(f"Equal split for {loc}: Using {num_files_per_category} MD files and {num_files_per_category} non-MD files")
        
        harmonized_files = harmonized_files_md + harmonized_files_non_md

        #### END FILTER BY WEEKDAY
        masks_files = extract_masks_files(f"gs://ps-imgs-mai1/{loc}", loc)
        masks_files = [
            s
            for s in masks_files
            if any(s.split("/")[-1].startswith(str(year)) for year in range(2020, 2030))
            and not any(
                s.split("/")[-1].startswith(exclude)
                for exclude in ["201701", "201702", "201703", "201704"]
            )
        ]

        def extract_substringID(item):
            """Extract substring between the last '/' and '_3B'"""
            match = re.search(r'/([^/]+)_3B', item)
            return match.group(1) if match else None
        
        def filter_list1_by_list2(list1, list2):
            """Remove items from list1 where substring is not found in any item in list2"""
            
            # Extract all substrings from list2 for comparison
            list2_substrings = set()
            for item in list2:
                substring = extract_substringID(item)
                if substring:
                    list2_substrings.add(substring)
            
            # Filter list1 - keep only items whose substring exists in list2_substrings
            filtered_list1 = []
            for item in list1:
                substring = extract_substringID(item)
                if substring and substring in list2_substrings:
                    filtered_list1.append(item)
            
            return filtered_list1
            
        harmonized_files = filter_list1_by_list2(harmonized_files, masks_files) # Keep only harmonized files for which the mask exists
        masks_files = filter_list1_by_list2(masks_files,harmonized_files) # keep only masks for which the harmonized files are sampled
            
        logger.debug("masks_files: %d, %s", len(masks_files), masks_files[0:5])

        # Process JSON files and create FeatureCollection
        fs = gcsfs.GCSFileSystem()
        with fs.open(f"gs://ps-imgs-mai1/imgProperties/{loc}.geojson") as f:
            geojson = gpd.read_file(f)

        exclude_list = ["2018", "2019", "2016", "2017"]

        # Create a regular expression pattern to match the start of the string
        pattern = r"^(" + "|".join(re.escape(item) for item in exclude_list) + ")"

        # Ensure all values in 'image_ID' are strings and handle missing values
        geojson["image_ID"] = geojson["image_ID"].fillna("").astype(str)

        # Filter the GeoDataFrame to exclude rows where 'image_ID' starts with any substring in exclude_list
        geojson = geojson[~geojson["image_ID"].str.match(pattern)]

        properties_to_keep = ["acquired", "clear_percent", "instrument", "image_ID"]

        geojson["acquired"] = geojson["acquired"].apply(format_acquired_date)

        geojson_str = geojson[properties_to_keep + ["geometry"]].to_json(
            cls=CustomJSONEncoder
        )
        geojson_dict = json.loads(geojson_str)
        image_ids = [feature["properties"]["image_ID"] for feature in geojson_dict["features"]]

        def filter_matching_files(image_ids, harmonized_files):
            # Extract harmonized ids (dict to keep full path for each id)
            harmonized_dict = {
                f.split("/")[-1].split("_3B")[0]: f for f in harmonized_files
            }
        
            # Keep only those that exist in both
            return [harmonized_dict[img] for img in image_ids if img in harmonized_dict]

        def filter_geojson_by_harmonized_files(geojson_dict, harmonized_files):
            # Extract harmonized ids for quick lookup
            harmonized_ids = {
                f.split("/")[-1].split("_3B")[0] for f in harmonized_files
            }
            
            # Filter features to keep only those with matching image_IDs
            filtered_features = [
                feature for feature in geojson_dict["features"]
                if feature["properties"]["image_ID"] in harmonized_ids
            ]
            
            # Update the geojson_dict with filtered features
            geojson_dict["features"] = filtered_features
            
            return geojson_dict
        
        harmonized_files = filter_matching_files(image_ids, harmonized_files)
        geojson_dict = filter_geojson_by_harmonized_files(geojson_dict, harmonized_files)
        #print('harmonized_files:', len(harmonized_files), harmonized_files[0:3])

        # Convert GeoJSON to JavaScript code
        js_code = geojson_to_ee_featurecollection(geojson_dict)
        logger.debug("Generated JS code")
        # print(harmonized_files[0:5])

        update_storage_class('ps-imgs-mai1', harmonized_files, "STANDARD", loc)
        update_storage_class('ps-imgs-mai1', masks_files, "STANDARD", loc)        
        
        imgs_and_props = f"./temp/{loc}_forRequire.txt"
        with open(f"./MAI/latest/template_require", "r") as fin:
            with open(imgs_and_props, "w") as fout:
                for line in fin:
                    fout.write(
                        line.replace("ENTER_URIS_HERE", "',\n '".join(harmonized_files))
                        .replace("ENTER_PROPERTIES_HERE", js_code)
                        .replace("ENTER_MASKS_HERE", "',\n '".join(masks_files))
                    )

        # Save the JavaScript code to a file
        codeFile = f"./temp/_activityPrepUpdate_gcs_{loc}.js"
        with open(f"./MAI/latest/{activityPrepCodeFile}gcs", "r") as fin:
            with open(codeFile, "w") as fout:
                for line in fin:
                    fout.write(
                        line.replace("INSERT_LOC_GROUP_HERE", locGroup)
                        .replace('"INSERT_LOC_HERE"', loc)
                        .replace("INSERT_BUCKET_HERE", GEEbucket)
                        .replace("INSERT_COUNTRY_HERE", country.replace(" ", ""))
                        .replace("INSERT_START_DATE_HERE", startDate)
                        .replace("INSERT_DATE_ID_HERE", dateID)
                        .replace("ENTERNODEHERE", nodePath.replace("bin", "lib"))
                    )

    # run the GEE code and update the location file
    subprocess.call([nodePath, "--no-deprecation", eerunnerPath, codeFile, f"--project={GEEproject}"])
    startUpdateProcess(loc, "activityPrepUpdate", setup, dateID, startDate, endDate)
    logger.debug(f"03 Activity prep for {loc} in {locGroup} started.")


def tryActivityExportUpdate(
    locGroup,
    loc,
    dateID,
    country,
    GEEbucket,
    activityExportCodeFile,
    setup,
    setupActivityPrep,
    GEEproject
):
    # runs logical checks to see if market activity export needs to be run for a given loc and locGroup,
    # runs the export if necessary, and updates the location file accordingly.

    startDate = checkUpdateProcessStatus(
        loc, "activityPrepUpdate", setupActivityPrep, dateID, "updateStartDate"
    ).strftime("%Y-%m-%d")
    endDate = checkUpdateProcessStatus(
        loc, "activityPrepUpdate", setupActivityPrep, dateID, "updateEndDate"
    ).strftime("%Y-%m-%d")

    # initialize google storage client
    client = storage.Client()

    # if marked complete or failed, exit
    if checkUpdateProcessStatus(loc, "activityExportUpdate", setup, dateID) in [
        "complete",
        "failed",
    ]:
        logger.debug(
            f"04 Activity export update already concluded for {loc} in {locGroup} -- skipping."
        )
        return

    # if mapping asset from previous step not present, exit
    if (
        checkUpdateProcessStatus(loc, "activityPrepUpdate", setupActivityPrep, dateID)
        != "complete"
    ):
        logger.debug(
            f"03 Activity prep update not complete for {loc} in {locGroup} -- skipping."
        )
        return

    # check if output already exists in GCS -- if so, exit and update
    storageOutput = str(
        list(client.list_blobs("exports-mai2023", prefix=f"{locGroup}/measures/"))
    )

    if f"{setup}_maxpMax{loc}_w7{dateID}" in storageOutput:
        if not checkUpdateProcessStatus(
            loc, "activityExportUpdate", setup, dateID, column="Status"
        ):
            print(
                f"Tried to complete a process record that doesn't exist: loc: {loc}, process: activityExportUpdate, dateID: {dateID}. Creating new process record."
            )
            startUpdateProcess(
                loc, "activityExportUpdate", setup, dateID, startDate, endDate
            )
        updateUpdateProcess(
            loc,
            "activityExportUpdate",
            setup,
            dateID,
            "Status",
            "complete",
            replace=True,
        )
        print(
            f"04 Activity export update succeeded for {loc} in {locGroup} -- updating location file."
        )
        return

    # check if task is already running -- if so, exit
    running = str(
        subprocess.check_output(["earthengine task list --status RUNNING"], shell=True)
    )
    ready = str(
        subprocess.check_output(["earthengine task list --status READY"], shell=True)
    )

    if f"{setup}_{loc}" in running + ready:
        logger.debug(f"04 Activity export currently running for {loc} in {locGroup}.")
        return

    # set up and modify GEE code to prepare for running
    codeFile = f"./temp/_exportActivity_{loc}.js"
    with open(f"./MAI/latest/{activityExportCodeFile}", "r") as fin:
        with open(codeFile, "w") as fout:
            for line in fin:
                fout.write(
                    line.replace("INSERT_LOC_GROUP_HERE", locGroup)
                    .replace('"INSERT_LOC_HERE"', loc)
                    .replace("INSERT_BUCKET_HERE", GEEbucket)
                    .replace("INSERT_COUNTRY_HERE", country)
                    .replace("INSERT_DATE_ID_HERE", dateID)
                )

    # run the GEE code and update the location file
    subprocess.call([nodePath, "--no-deprecation", eerunnerPath, codeFile, f"--project={GEEproject}"])
    startUpdateProcess(loc, "activityExportUpdate", setup, dateID, startDate, endDate)
    logger.debug(f"04 Activity export update for {loc} in {locGroup} started.")


# ---------------------------------------------------#
# Activity Update Functions
# ---------------------------------------------------#


def activityUpdateRunner(name):
    print("activityUpdateRunner started...")

    while True:
        locs = getLocsForActivityUploadUpdate(name)

        if locs:
            print(f"{len(locs)} locs for activity upload: {locs[0:5]}...")
            for loc, dateID, startDate, endDate in locs:
                updateActivity(loc, dateID, startDate, endDate)

        time.sleep(120)



# ---------------------------------------------------#
# Helper Functions
# ---------------------------------------------------#

def update_storage_class(bucket_name, object_names, new_storage_class="STANDARD", loc=None):
    """Update the storage class of a list of blobs in a bucket, only if different.
       Prints summary of changes at the end."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    updated_count = 0
    skipped_count = 0
    failed_count = 0

    for obj_name in object_names:
        # Construct full blob path (your convention with _clip_file_format.tif)
        blob_name = f"{loc}/{obj_name}_clip_file_format.tif" if loc else obj_name
        blob = bucket.blob(blob_name)

        try:
            # Reload metadata to get current storage class + generation
            blob.reload()

            if blob.storage_class == new_storage_class:
                #print(f"⏩ Skipped {blob_name} (already {new_storage_class})")
                skipped_count += 1
                continue

            gen_match = blob.generation
            old_class = blob.storage_class
            blob.update_storage_class(new_storage_class, if_generation_match=gen_match)

            #print(f"🔄 Updated {blob_name} from {old_class} → {new_storage_class}")
            updated_count += 1

        except Exception as e:
            #print(f"❌ Failed to update {blob_name}: {e}")
            failed_count += 1

    # Print final summary
    total = len(object_names)
    print(f"\n📊 Update Summary {loc} ✅ Updated: {updated_count}  ⏩ Skipped: {skipped_count}  ❌ Failed:  {failed_count}  Total processed: {total}")


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

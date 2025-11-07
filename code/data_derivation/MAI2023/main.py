import ee, os, re, json, time, requests, subprocess, random, threading, geojson, mysql.connector, logging, warnings, traceback, gcsfs
from datetime import datetime
from shapely.geometry import shape
import pandas as pd
from IPython.display import display
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from MAI2023.src.dbFunctions import *
from MAI2023.src.activityFunctions import *
from MAI2023.src.bucketFunctions import *
from tqdm.notebook import tqdm  # Import tqdm for progress bar
from google.cloud import storage

# -------------------------------------------------------------------------------------------------------------------------------
# GLOBAL VARIABLES
# -------------------------------------------------------------------------------------------------------------------------------

warnings.filterwarnings("ignore")
storage_client = storage.Client(project="planetupload")
bucketName = "mai_2023"
bucket = storage_client.get_bucket(bucketName)
order_url = "https://api.planet.com/compute/ops/orders/v2"
search_url = "https://api.planet.com/data/v1/quick-search"
#with open("./MAI2023/cred.txt", "r") as f:
#    cred = f.readlines()[0]
file_lock = threading.Lock()
session = requests.Session()
colspecs = [(0, 24), (26, 38), (40, 82), (84, 93), (95, 1000)]
max_retries = 10
retry_interval = 5
endDate = datetime.today().strftime("%Y-%m-%d")
date_pattern1 = r"_20\d{2}-\d{2}-\d{2}_"
date_pattern2 = r"PSScene/20\d{2}\d{2}\d{2}_"
#nodePath = str(subprocess.check_output(["which node"], shell=True))[2:-3]
#eerunnerPath = str(subprocess.check_output(["which ee-runner"], shell=True))[2:-3]
allowed_properties = {
    "acquired",
    "anomalous_pixels",
    "clear_confidence_percent",
    "clear_percent",
    "cloud_percent",
    "gsd",
    "heavy_haze_percent",
    "instrument",
    "light_haze_percent",
    "satellite_azimuth",
    "satellite_id",
    "view_angle",
    "visible_confidence_percent",
    "visible_percent",
}

logger = logging.getLogger("logging")

ee.Initialize()


# Custom JSON encoder for Timestamps
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        return super().default(obj)


# -------------------------------------------------------------------------------------------------------------------------------
# MASTER RUNNER
# -------------------------------------------------------------------------------------------------------------------------------


def run_master(
    name,
    private_key,
    prepCodeFile,
    shapeCodeFile,
    activityPrepCodeFile,
    activityExportCodeFile,
    setupImg,
    setupMap,
    setupActivityPrep,
    setupActivity,
    maxCloudCover=0.5,
    maxRunningDownloads=30,
    signalCutoff=20,
    focalMinRadius=2,
    debug=False,
    GEEproject="kenya3",
    processes=[ "processing", 'downloading'],
    use_wb_key=False
):
    # set up logger config
    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO)

    # authorize planet API session
    # set up API key:
    load_dotenv(dotenv_path=os.path.expanduser("~/.env") if os.geteuid() != 0 else "/root/MAI2023/.env")

    if use_wb_key:
        PLANET_API_KEY = os.environ.get("PLANET_API_KEY_WB")
        print("="*40)
        print(f"WARNING USING WORLD BANK KEY: {PLANET_API_KEY}")
        print("="*40)
    else:  
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
        target=downloadThreader,
        args=(
            name,
            endDate,
            private_key,
            setupImg,
            setupMap,
            setupActivityPrep,
            setupActivity,
            maxRunningDownloads,
            maxCloudCover,
            5,
            use_wb_key
        ),
    )
    procThread = threading.Thread(
        target=processor,
        args=(
            name,
            prepCodeFile,
            shapeCodeFile,
            activityPrepCodeFile,
            activityExportCodeFile,
            setupImg,
            setupMap,
            setupActivityPrep,
            setupActivity,
            signalCutoff,
            focalMinRadius,
            GEEproject,
        ),
    )
    actThread = threading.Thread(target=activityRunner, args=(name, signalCutoff))

    # start the threads
    if "downloading" in processes:
        downThread.start()
    if "processing" in processes:
        procThread.start()
        actThread.start()

    # join the threads to wait for their completion
    if "downloading" in processes:
        downThread.join()
    if "processing" in processes:
        procThread.join()
        actThread.join()


# -------------------------------------------------------------------------------------------------------------------------------
# DOWNLOADER FUNCTIONS
# -------------------------------------------------------------------------------------------------------------------------------


def downloadThreader(
    name,
    endDate,
    private_key,
    setupImg,
    setupMap,
    setupActivityPrep,
    setupActivity,
    maxRunningDownloads,
    maxCloudCover,
    max_threads=5,
    use_wb_key=False
):
    print("Downloading started...")

    # pull list of locs that need downloads
    locList = getAssignedLocs(
        name,
        setupImg,
        setupMap,
        setupActivityPrep,
        setupActivity,
        status="for_download",
    )
    print(f"{len(locList)} locs for download before:", locList)
    runningLocs = []

    # while there are still any locs that need downloads
    while locList:
        # while active threads < 5 and locs are in the list that aren't in an active thread
        while len(runningLocs) < max_threads and [
            loc for loc in locList if loc not in runningLocs
        ]:
            # get the first loc in the list that isn't in an active thread
            loc = [loc for loc in locList if loc not in runningLocs][0]

            # start a new download thread with the loc
            thread = threading.Thread(
                target=downloader,
                args=(
                    name,
                    loc,
                    setupImg,
                    setupMap,
                    setupActivityPrep,
                    setupActivity,
                    runningLocs,
                    endDate,
                    private_key,
                    maxRunningDownloads,
                    maxCloudCover,
                    use_wb_key
                ),
            )
            thread.start()
            print(f"Started download thread for {loc}")

            # add that location to the list of locs with an active thread
            runningLocs.append(loc)
            print(f"Currently running download threads: {runningLocs}")

            time.sleep(5)

        time.sleep(30)

        # update the list of locs that need downloads
        locList = getAssignedLocs(
            name,
            setupImg,
            setupMap,
            setupActivityPrep,
            setupActivity,
            status="for_download",
        )
        # print(f'{len(locList)} locs for download after:',locList)


def downloader(
    name,
    loc,
    setupImg,
    setupMap,
    setupActivityPrep,
    setupActivity,
    runningLocs,
    endDate,
    private_key,
    maxRunningDownloads,
    maxCloudCover,
    use_wb_key
):
    # loop and request downloads

    # get current location status
    locGroup = checkLocationFileStatus(loc, "locGroup")
    bucket = checkLocationFileStatus(loc, "bucket")

    if not bucket or bucket == "" or checkLocationFileStatus(loc, "stored_in_gcs") == 1:
        bucket = "p155mali3"
        if checkLocationFileStatus(loc, "stored_in_gcs") == 0:
            print(f"No bucket assigned to {loc} -- adding to emptiest bucket...")
            add_loc_to_bucket(loc)
        if checkLocationFileStatus(loc, "stored_in_gcs") == 1:
            updateLocationFileStatus(loc, "bucket", bucket, replace=True)
            try:
                ee.data.createAsset(
                    {"type": "Folder"},
                    f"projects/{bucket}/assets/PS_imgs/155_Mali/{loc}proc",
                )
            except ee.ee_exception.EEException as e:
                if "Cannot overwrite asset" in str(e):
                    pass
                else:
                    raise e

            # create new image collection in destination bucket
            try:
                ee.data.createAsset(
                    {"type": "ImageCollection"},
                    f"projects/{bucket}/assets/PS_imgs/155_Mali/{loc}",
                )
            except ee.ee_exception.EEException as e:
                if "Cannot overwrite asset" in str(e):
                    pass
                else:
                    raise e

            # check whether assets were created correctly
            assetList = ee.data.listAssets(
                f"projects/{bucket}/assets/PS_imgs/155_Mali"
            )["assets"]
            if [asset["id"] for asset in assetList if loc in asset["id"]]:
                print(f"successfully added {loc} to bucket {bucket}")
            else:
                raise Exception(f"failed to add {loc} to bucket {bucket}")

        print(f"No bucket assigned to {loc} -- adding to {bucket}...")
        time.sleep(5)

    ### Extract json of each feature, convert to convex hull geometry, and export to temp folder as a json

    # pull jsons from GCS
    saveConvexHull(loc)

    # while loc downloads not completed or failed
    while loc in getAssignedLocs(
        name,
        setupImg,
        setupMap,
        setupActivityPrep,
        setupActivity,
        status="for_download",
    ):
        # find locs whose imagery downloads are processing and check if they have finished:
        if checkLocationFileStatus(loc, "00DownStatus") == "initiated":
            fn_checkExistingImages(
                loc,
                locGroup,
                endDate,
                maxCloudCover,
                setupImg,
                setupMap,
                setupActivityPrep,
                setupActivity,
                use_wb_key
            )

            # check if the location has reached the attempted downloads limit -- if so, mark as failed
            if checkLocationFileStatus(loc, "00bDownloadsInitiated") > 4:
                print(
                    f"{loc} reached download limit without success -- marking as failed."
                )
                updateLocationFileStatus(loc, "00DownStatus", "failed", replace=True)
                display(locationFileSummary(loc))

        # if no orders are currently running for this location
        checked = checkRunningOrders(use_wb_key)
        if loc not in checked and loc in getAssignedLocs(
            name,
            setupImg,
            setupMap,
            setupActivityPrep,
            setupActivity,
            status="for_download",
        ):
            requestDownloads(
                loc,
                locGroup,
                endDate,
                private_key,
                maxRunningDownloads,
                maxCloudCover,
                setupImg,
                setupMap,
                setupActivityPrep,
                setupActivity,
                use_wb_key
            )

        time.sleep(10)

    print(f"All downloads complete for {loc}!")
    runningLocs.remove(loc)
    print(f"Currently running download threads: {runningLocs}")


def requestDownloads(
    loc,
    locGroup,
    endDate,
    private_key,
    maxRunningDownloads,
    maxCloudCover,
    setupImg,
    setupMap,
    setupActivityPrep,
    setupActivity,
    use_wb_key
):
    # Function to request the downloads needed for a given location.
    # Inputs:
    # new_products [list]:      list of sr-corrected image product ids that should be downloaded from Planet
    # forAnchoring [string]:    planet image product id that should be used for anchoring the downloaded images
    # private_key [string]:     file path to planet encrypted private key

    if use_wb_key:
        PLANET_API_KEY = os.environ.get("PLANET_API_KEY_WB")
    else:  
        PLANET_API_KEY = os.environ.get("PLANET_API_KEY")
    session = requests.Session()
    session.auth = (PLANET_API_KEY, "")

    new_products, forAnchoring = fn_checkExistingImages(
        loc,
        locGroup,
        endDate,
        maxCloudCover,
        setupImg,
        setupMap,
        setupActivityPrep,
        setupActivity,
        use_wb_key
    )
    GEEbucket = checkLocationFileStatus(loc, "bucket")
    locGroup = checkLocationFileStatus(loc, "locGroup")
    logger.debug(f"New SR products found: {new_products[0:5]} ...")
    orders = []

    with open(f"./temp/Jsons/{loc}feature.geojson") as f:
        geojson_data = json.loads(f.read())
    lenCurrRunTasks = 100
    # while lenCurrRunTasks>10:
    currRunnTasks = list(checkRunningOrders(use_wb_key))
    lenCurrRunTasks = len(currRunnTasks)

    # if we haven't already, record available images at different levels of cloud cover
    if not (
        getJSON(loc, "totalAvailable", "cloud_0")
        and getJSON(loc, "totalAvailable", "cloud_25")
        and getJSON(loc, "totalAvailable", "cloud_50")
        and getJSON(loc, "totalAvailable", "cloud_75")
        and getJSON(loc, "totalAvailable", "cloud_100")
    ):
        # Record available images at various levels of cloud cover
        features_sr_0 = searchAvailableImgs(geojson_data, endDate, 0, use_wb_key)
        updateJSON(
            loc,
            "totalAvailable",
            "cloud_0",
            int(len(features_sr_0)),
        )

        features_sr_25 = searchAvailableImgs(geojson_data, endDate, 0.25, use_wb_key)
        updateJSON(
            loc,
            "totalAvailable",
            "cloud_25",
            int(len(features_sr_25)),
        )

        features_sr_50 = searchAvailableImgs(geojson_data, endDate, 0.5, use_wb_key)
        updateJSON(
            loc,
            "totalAvailable",
            "cloud_50",
            int(len(features_sr_50)),
        )

        features_sr_75 = searchAvailableImgs(geojson_data, endDate, 0.75, use_wb_key)
        updateJSON(
            loc,
            "totalAvailable",
            "cloud_75",
            int(len(features_sr_75)),
        )

        features_sr_100 = searchAvailableImgs(geojson_data, endDate, 1, use_wb_key)
        updateJSON(
            loc,
            "totalAvailable",
            "cloud_100",
            int(len(features_sr_100)),
        )

    if lenCurrRunTasks > maxRunningDownloads:
        print(
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

        if len(new_products) >= 10:
            print(
                f"SR download initiated for {loc} -- requesting {len(new_products)} products"
            )
            updateLocationFileStatus(loc, "00DownStatus", "initiated", replace=True)

            for i in range(0, len(new_products), 499):
                logger.debug(f"Chunk {i}")
                itemIDs = []
                itemIDs.extend(new_products[i : i + 499])
                itemIDs.append(str(forAnchoring))

                bucketFolder = ee.data.listAssets(
                    f"projects/{GEEbucket}/assets/PS_imgs"
                )["assets"][0]["id"].split("/")[-1]

                order_payload = fn_order_payload()
                order_payload["products"][0]["item_ids"] = itemIDs
                order_payload["name"] = f"{loc} chunk {i}"
                order_payload["delivery"]["google_cloud_storage"]["bucket"] = (
                    f"ps-imgs-mai1"
                )
                order_payload["delivery"]["google_cloud_storage"]["path_prefix"] = loc
                order_payload["delivery"]["google_cloud_storage"]["credentials"] = (
                    private_key
                )
                order_payload["tools"][0]["clip"]["aoi"] = geojson_data["geometry"]
                #order_payload["tools"][3]["coregister"]["anchor_item"] = forAnchoring

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
                        # updateLocationFileStatus(loc, "00DownStatus", "complete", replace=True)

                # time.sleep(600) #120
                time.sleep(60)

    # locationFileSummary(locGroup, loc)


def fn_checkExistingImages(
    loc,
    locGroup,
    endDate,
    maxCloudCover,
    setupImg,
    setupMap,
    setupActivityPrep,
    setupActivity,
    use_wb_key
):
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

    if use_wb_key:
        PLANET_API_KEY = os.environ.get("PLANET_API_KEY_WB")
    else:  
        PLANET_API_KEY = os.environ.get("PLANET_API_KEY")
    session = requests.Session()
    session.auth = (PLANET_API_KEY, "")

    logger.debug(f"Checking existing images for {loc} in {locGroup} up to {endDate}:")

    # If image downloads already marked complete, don't run.
    if checkLocationFileStatus(loc, "00DownStatus") == "complete":
        print("All downloads marked complete for", loc, " -- skipping image check.")
        new_products, forAnchoring = ("none", "none")
        return new_products, forAnchoring

    # If image downloads already running, don't run.
    locsRunningOrders = checkRunningOrders(use_wb_key)
    logger.debug(f"locs with running Planet orders: {set(locsRunningOrders)}")
    if loc in locsRunningOrders:
        logger.debug(f"Downloads currently running for {loc} -- skipping image check.")
        new_products, forAnchoring = ("none", "none")
        time.sleep(180)

    else:
        # search for collections that already exist for the location, and store them in a list
        logger.debug(f"Looking up existing imagery for {loc}...")
        pattern = r"{}/(.*?)_3B_".format(loc)  # pattern to search for
        tif_pattern = r"\.tif$"  # Matches strings ending with .tif
        latestDate = "2016-01-01"
        name = f"{locGroup}/loc{loc}/"
        if checkLocationFileStatus(loc, "stored_in_gcs") == 0:
            GEEbucket = checkLocationFileStatus(loc, "bucket")
            bucketFolder = ee.data.listAssets(f"projects/{GEEbucket}/assets/PS_imgs")[
                "assets"
            ][0]["id"].split("/")[-1]
            os.system(
                f'earthengine ls "projects/{GEEbucket}/assets/PS_imgs/{bucketFolder}/{loc}"> ./temp/alreadyUp{loc}.txt'
            )

        if checkLocationFileStatus(loc, "stored_in_gcs") == 1:
            os.system(
                f"gcloud storage ls --recursive gs://ps-imgs-mai1/{loc} > ./temp/alreadyUp{loc}.txt"
            )
        else:
            updateLocationFileStatus(loc, "stored_in_GCS", 1, replace=True)
            os.system(
                f"gcloud storage ls --recursive gs://ps-imgs-mai1/{loc} > ./temp/alreadyUp{loc}.txt"
            )
        existing = []
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

        with open(f"./temp/Jsons/{loc}feature.geojson") as f:
            geojson_data = json.loads(f.read())  # open up the convex hull json

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
        features_sr = searchAvailableImgs(geojson_data, endDate, maxCloudCover, use_wb_key)

        # Retrieve the product IDs from the search response that we don't already have
        product_ids = []
        for i in features_sr:
            product_ids.append(i["id"])
        new_products = remove_overlapping_strings(product_ids, existing)

        logger.debug(f"{loc} total number of products available: {len(product_ids)}")
        logger.debug(f"{loc} total number of existing products: {len(existing)}")
        logger.debug(f"{loc} number of new products available: {len(new_products)}")

        # If the number of new products is less than 5 for both download types, mark the location complete
        if len(new_products) < 10:
            print(f"All images already downloaded for {loc} -- marking complete")

            # delete duplicate images
            if checkLocationFileStatus(loc, "stored_in_gcs") == 1:
                deleteDuplicates_gcs(loc)
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
            if checkLocationFileStatus(loc, "stored_in_gcs") == 0:
                deleteDuplicates(loc)

            updateLocationFileStatus(loc, "00DownStatus", "complete", replace=True)
            assignForProcessing(
                loc,
                setupImg,
                setupMap,
                setupActivityPrep,
                setupActivity
            )
            print(f"Downloading complete for {loc}")

    return new_products, forAnchoring


def searchAvailableImgs(geojson_data, endDate, maxCloudCover, use_wb_key):
    logger.debug(f"Requesting available images for cloud cover {maxCloudCover}...")

    if use_wb_key:
        PLANET_API_KEY = os.environ.get("PLANET_API_KEY_WB")
    else:  
        PLANET_API_KEY = os.environ.get("PLANET_API_KEY")
    session = requests.Session()
    session.auth = (PLANET_API_KEY, "")

    # Create new search parameters to capture all images of interest
    search_para_2 = fn_search_para_2()
    search_para_2["filter"]["config"][0]["config"]["coordinates"] = geojson_data[
        "geometry"
    ]["coordinates"]
    search_para_2["filter"]["config"][1]["config"]["gte"] = "2016-01-01T00:00:00Z"
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


def checkRunningOrders(use_wb_key):
    load_dotenv(
        dotenv_path=os.path.expanduser("~/.env")
        if os.geteuid() != 0
        else "/root/MAI2023/.env"
    )

    if use_wb_key:
        PLANET_API_KEY = os.environ.get("PLANET_API_KEY_WB")
    else:  
        PLANET_API_KEY = os.environ.get("PLANET_API_KEY")

    url = "https://api.planet.com/compute/ops/orders/v2?state=running&state=queued"
    headers = {
        "Authorization": f"api-key {PLANET_API_KEY}",
        "Content-Type": "application/json"
    }
    orders = []
    while url:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        # Filter for only running or queued orders
        filtered = [o['name'].split(' ')[0] for o in data.get('orders', []) if o.get('state') in ['running', 'queued']]
        orders.extend(filtered)
        url = data.get('_links', {}).get('next')
    
    return orders


def checkRunningOrders_new():
    # Uncomment to authorize in this function
    PLANET_API_KEY = os.environ.get("PLANET_API_KEY_WB")
    session = requests.Session()
    session.auth = (PLANET_API_KEY, "")

    DONE = False  # Initialize DONE
    while not DONE:
        time.sleep(random.randint(0, 10))  # To prevent concurrent requests
        try:
            # Get initial list of orders
            response = session.get("https://api.planet.com/compute/ops/orders/v2")
            response.raise_for_status()  # Ensure a successful response
            orders_list = response.json()
            # print(orders_list)
            # orders_list = [order for order in orders_list['orders'] if order['state'] in ('running', 'queued')]
            all_orders = orders_list.get("orders", [])
            # print(all_orders)
            # Go over pages to capture all orders
            while "_links" in orders_list and "next" in orders_list["_links"]:
                next_response = session.get(orders_list["_links"]["next"])
                next_response.raise_for_status()
                orders_list = next_response.json()
                all_orders.extend(orders_list.get("orders", []))
                time.sleep(1)

            loc_list = []

            # Filter running/queued orders for Google Cloud Storage
            gcs_orders = [
                order
                for order in all_orders
                if "google_cloud_storage" in order.get("delivery", {})
                and order["state"] in ["running", "queued"]
            ]
            if gcs_orders:
                loc_list.extend(
                    order["delivery"]["google_cloud_storage"]["path_prefix"]
                    for order in gcs_orders
                )

            # Filter running/queued orders for Google Earth Engine
            gee_orders = [
                order
                for order in all_orders
                if "google_earth_engine" in order.get("delivery", {})
                and order["state"] in ["running", "queued"]
            ]
            if gee_orders:
                loc_list.extend(
                    order["delivery"]["google_earth_engine"]["path_prefix"]
                    for order in gee_orders
                )

            DONE = True  # Set DONE to True when successful
            print(f"Succeeded checkRunningOrders for {loc}")
            return list(set(loc_list))  # Return unique locations

        except Exception as e:
            print(f"Failed checkRunningOrders for {loc}: {e}")
            time.sleep(5)  # Wait before retrying


# ------------------------------------------------------------------------------------------------------------------------------
# PROCESSOR FUNCTIONS
# ------------------------------------------------------------------------------------------------------------------------------


def processor(
    name,
    prepCodeFile,
    shapeCodeFile,
    activityPrepCodeFile,
    activityExportCodeFile,
    setupImg,
    setupMap,
    setupActivityPrep,
    setupActivity,
    signalCutoff,
    focalMinRadius,
    GEEproject,
):
    # loops through a given list of locations within a locGroup to run checks and, if necessary, processing tasks
    # continues to loop continuously until all tasks have been concluded for all locations in the list

    print("Processing started...")

    # while there are any assigned locations not yet fully processed
    while True:  # getAssignedLocs(name, setupImg, setupMap, setupActivityPrep, setupActivity, status='for_process'):
        # get a list of locations that have completed downloading, but not finished processing
        locList = getAssignedLocs(
            name,
            setupImg,
            setupMap,
            setupActivityPrep,
            setupActivity,
            status="for_process",
        )  # FOLLOW
        random.shuffle(locList)
        print(f"{len(locList)} locs for process:", locList)
        # if there are any locations ready to process
        if locList:
            # set up frequency based on input
            freqListStr = "weekday"  # weekday monthday, monthdayfromEnd, weekdayEverySecond,everyFiveDays
            if freqListStr == "weekday":
                freqList = 6
                freqListStr_short = "w7"
            elif freqListStr == "monthday" or freqListStr == "monthdayfromEnd":
                freqList = 30
                freqListStr_short = "w31"
            elif freqListStr == "weekdayEverySecond":
                freqList = 13
                freqListStr_short = "w14"
            elif freqListStr == "everyFiveDays":
                freqList = 4
                freqListStr_short = "w5"

            client = storage.Client()

            # loop through the locations
            for loc in locList:
                try:
                    logger.debug(f"Beginning processing loop for {loc}...")
                    locGroup = checkLocationFileStatus(loc, "locGroup")
                    country = checkLocationFileStatus(loc, "country")
                    GEEbucket = checkLocationFileStatus(loc, "bucket")

                    completedTasks = checkCompletedTasksGEE(loc)

                    if completedTasks:
                        lastCompletedTask = completedTasks[0]
                        
                        # Lookup to determine which errors to record in the DB
                        with open("./MAI2023/familiar_errors_lookup.json", "r") as f:
                            error_lookup = json.load(f)
                        
                        if "FAILED" in lastCompletedTask:
                            error_text = lastCompletedTask.split("FAILED")[-1].strip()
                            logger.debug(f'WARNING: {loc} failed with error message:', error_text)
                            failedProcess, failedSetup = processFromGEEDescription(lastCompletedTask, loc=loc, setup=True)
                            if failedSetup!="":
                                # Mark tasks with existing outputs as complete
                                if "Cannot overwrite asset" in lastCompletedTask:
                                    updateProcess(loc, failedProcess, failedSetup, "complete")
    
                                # Mark familiar failed tasks in the DB accordingly
                                elif any([e in error_text for e in error_lookup[failedProcess]]):
                                    print(f"{failedProcess} for {loc} in {locGroup} failed due to unhandled error: {error_text}")                                
                                    updateProcess(loc, failedProcess, failedSetup, 'failed', fail_reason=error_text.replace("'", ''), column="Status")
                                    continue

                    # try each processing step
                    tryPropertiesExport(locGroup, loc, GEEbucket, GEEproject)
                    tryExportPrep(
                        locGroup,
                        loc,
                        freqList,
                        freqListStr,
                        GEEbucket,
                        country,
                        prepCodeFile,
                        GEEproject,
                        setup=setupImg,
                    )
                    tryExportMarketShape(
                        locGroup,
                        loc,
                        freqList,
                        freqListStr,
                        GEEbucket,
                        country,
                        shapeCodeFile,
                        GEEproject,
                        setup=setupMap,
                        setupImg=setupImg,
                        focalMinRadius=focalMinRadius,
                    )
                    tryActivityPrep(
                        locGroup,
                        loc,
                        country,
                        GEEbucket,
                        activityPrepCodeFile,
                        GEEproject,
                        setup=setupActivityPrep,
                        setupMap=setupMap,
                        signalCutoff=signalCutoff,
                    )
                    tryExportMarketActivity(
                        locGroup,
                        loc,
                        country,
                        GEEbucket,
                        activityExportCodeFile,
                        GEEproject,
                        setup=setupActivity,
                        setupMap=setupMap,
                        setupActivityPrep=setupActivityPrep,
                    )
                    time.sleep(2)

                except Exception as e:
                    print(f"Problem in processor with {loc}", e)
                    traceback.print_exc()

            # wait in between loops through all the locations
            time.sleep(300)

            # re-update the locations for processing
            locList = getAssignedLocs(
                name,
                setupImg,
                setupMap,
                setupActivityPrep,
                setupActivity,
                status="for_process",
            )

        else:
            print("No locs to process currently -- waiting...")
            time.sleep(600)


def checkCompletedTasksGEE(loc="lon", search=[""]):
    # checks GEE task history to look for tasks for the given location, fc, hc, and additional search string.
    # by default, will search for all tasks with hc = 5, fc = 50, and failure due to empty geometry

    tasks = str(subprocess.check_output(["earthengine task list"], shell=True)).split(
        "\\n"
    )
    return [
        line.replace("\\", "")
        for line in tasks
        if (
            loc in line
            and any(status in line for status in ["FAILED", "COMPLETED"])
            and all(term in line for term in search)
        )
    ]


def tryPropertiesExport(locGroup, loc, GEEbucket, GEEproject):
    # runs logical checks to see if properties export needs to be run for a given loc and locGroup,
    # runs the export if necessary, and updates the location file accordingly.

    # initialize google storage client
    client = storage.Client()

    # if marked complete or failed, exit
    # if checkLocationFileStatus(loc, 'stored_in_gcs')==0:
    if checkProcessStatus(loc, "PropExport", "") in ["complete", "failed"]:
        logger.debug(
            f"Properties export already concluded for {loc} in {locGroup} -- skipping."
        )
        return

    # check if output already exists in the GCS bucket -- if so, exit and update
    if (
        checkLocationFileStatus(loc, "stored_in_gcs") == 0
        and loc
        in str(
            list(client.list_blobs("exports-mai2023", prefix=f"{locGroup}/properties/"))
        )
        or checkLocationFileStatus(loc, "stored_in_gcs") == 1
        and loc
        in str(
            list(client.list_blobs("ps-imgs-mai1", prefix=f"{locGroup}/properties/"))
        )
    ):
        if checkProcessStatus(loc, "PropExport", ""):
            updateProcess(loc, "PropExport", "", "complete")
        else:
            startProcess(loc, "PropExport", "")
            updateProcess(loc, "PropExport", "", "complete")
        print(
            f"Properties export succeeded for {loc} in {locGroup} -- updating location file."
        )
        return
    if checkLocationFileStatus(loc, "stored_in_gcs") == 0:
        # check if task is already running -- if so, exit and update
        if running_tasks(GEEproject, search_strings = [f"prop_{loc}"]):
            logger.debug(f"Properties export currently running for {loc}")
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
        subprocess.call(
            [
                nodePath,
                "--no-deprecation",
                eerunnerPath,
                codeFile,
                f"--project={GEEproject}",
            ]
        )
        startProcess(loc, "PropExport", setup="")
        print(
            f"Properties download for {loc} in {locGroup} initiated  -- updating location file."
        )

    if checkLocationFileStatus(loc, "stored_in_gcs") == 1:
        fs = gcsfs.GCSFileSystem()
        try:
            with fs.open(f"gs://ps-imgs-mai1/imgProperties/{loc}.geojson") as f:
                geojson = gpd.read_file(f)
        except:
            deleteDuplicates_gcs(loc)
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
            startProcess(loc, "PropExport", "")
            updateProcess(loc, "PropExport", "", "complete")

        df = pd.DataFrame(geojson.drop(columns="geometry"))
        with fs.open(
            f"gs://ps-imgs-mai1/{locGroup}/properties/propEx_{locGroup}_{loc}.csv", "w"
        ) as f:
            df.to_csv(f, index=False)
        startProcess(loc, "PropExport", "")
        updateProcess(loc, "PropExport", "", "complete")


# Write the DataFrame to a CSV file and upload it to GCS

# run the GEE code and update the location file


def tryExportPrep(
    locGroup,
    loc,
    freqList,
    freqListStr,
    GEEbucket,
    country,
    prepCodeFile,
    GEEproject,
    setup,
):
    # runs logical checks to see if export prep needs to be run for a given loc and locGroup,
    # runs the export prep if necessary, and updates the location file accordingly.

    logger.debug(f"Trying 01 Export prep for {loc} in {locGroup}.")

    # if marked complete or failed, skip
    if checkProcessStatus(loc, "01Prep", setup) in ["complete", "failed"]:
        logger.debug(
            f"01 Export prep already concluded for {loc} in {locGroup} -- skipping."
        )
        return

    # check if output already exists in GEE -- if so, exit and update
    bucketFolder = ee.data.listAssets(f"projects/{GEEbucket}/assets/PS_imgs")["assets"][
        0
    ]["id"].split("/")[-1]
    commandString = (
        f'earthengine ls "projects/{GEEbucket}/assets/PS_imgs/{bucketFolder}/{loc}proc"'
    )

    if f"diffImg{setup}" in str(subprocess.check_output([commandString], shell=True)):
        print(
            f"01 Export prep succeeded for {loc} in {locGroup} -- updating location file."
        )
        if checkProcessStatus(loc, "01Prep", setup):
            updateProcess(loc, "01Prep", setup, "complete")
        else:
            startProcess(loc, "01Prep", setup)
            updateProcess(loc, "01Prep", setup, "complete")
        return

    # check if task is already running -- if so, exit
    if running_tasks(GEEproject, search_strings = [f"diffImg{loc}", f"diffImg{loc}"]):
        logger.debug(f"01 Export prep currently running for {loc} in {locGroup}.")
        return

    if checkLocationFileStatus(loc, "stored_in_gcs") == 0:
        # set up and modify GEE code to prepare for running
        logger.debug(f"Building export prep codefile for {loc} in {locGroup}.")
        codeFile = f"./temp/_findMktsA_{loc}.js"
        with open(f"./MAI/latest/{prepCodeFile}", "r") as fin:
            with open(codeFile, "w") as fout:
                for line in fin:
                    fout.write(
                        line.replace("INSERT_LOC_GROUP_HERE", locGroup)
                        .replace('"INSERT_LOC_HERE"', loc)
                        .replace('"INSERT_FREQ_LIST_HERE"', str(freqList))
                        .replace("INSERT_FREQ_DAY_STR_HERE", freqListStr)
                        .replace("INSERT_BUCKET_HERE", GEEbucket)
                        .replace("INSERT_COUNTRY_HERE", country.replace(" ", ""))
                    )

        # run the GEE code and update the location file
        subprocess.call(
            [
                nodePath,
                "--no-deprecation",
                eerunnerPath,
                codeFile,
                f"--project={GEEproject}",
            ]
        )
        startProcess(loc, "01Prep", setup)
    if checkLocationFileStatus(loc, "stored_in_gcs") == 1:
        # Extract harmonized files and image IDs
        harmonized_files, _ = extract_harmonized_files_and_ids(
            f"gs://ps-imgs-mai1/{loc}", loc
        )
        logger.debug(
            "harmonized_files: %d, %s", len(harmonized_files), harmonized_files[0:5]
        )

        masks_files = extract_masks_files(f"gs://ps-imgs-mai1/{loc}", loc)

        # Process JSON files and create FeatureCollection
        fs = gcsfs.GCSFileSystem()
        with fs.open(f"gs://ps-imgs-mai1/imgProperties/{loc}.geojson") as f:
            geojson = gpd.read_file(f)
        properties_to_keep = ["acquired", "clear_percent", "instrument", "image_ID"]

        geojson["acquired"] = geojson["acquired"].apply(format_acquired_date)

        geojson_str = geojson[properties_to_keep + ["geometry"]].to_json(
            cls=CustomJSONEncoder
        )
        geojson_dict = json.loads(geojson_str)

        # Convert GeoJSON to JavaScript code
        js_code = geojson_to_ee_featurecollection(geojson_dict)
        logger.debug("Generated JS code")
        # print(harmonized_files[0:5])

        imgs_and_props = f"./temp/{loc}_forRequire.txt"
        with open(f"./MAI/latest/template_require", "r") as fin:
            with open(imgs_and_props, "w") as fout:
                for line in fin:
                    fout.write(
                        line.replace(
                            "ENTER_URIS_HERE",
                            "',\n '".join(
                                random.sample(
                                    harmonized_files, min(len(harmonized_files), 3000)
                                )
                            ),
                        )
                        .replace("ENTER_PROPERTIES_HERE", js_code)
                        .replace("ENTER_MASKS_HERE", "',\n '".join(masks_files))
                    )

        # Save the JavaScript code to a file
        codeFile = f"./temp/_findMktsA_{loc}.js"
        with open(f"./MAI/latest/{prepCodeFile}_gcs", "r") as fin:
            with open(codeFile, "w") as fout:
                for line in fin:
                    fout.write(
                        line.replace("INSERT_LOC_HERE", loc)
                        .replace("INSERT_LOC_GROUP_HERE", locGroup)
                        .replace("INSERT_BUCKET_HERE", GEEbucket)
                        .replace("INSERT_COUNTRY_HERE", country.replace(" ", ""))
                        .replace("ENTERNODEHERE", nodePath.replace("bin", "lib"))
                    )

        subprocess.call(
            [
                nodePath,
                "--no-deprecation",
                eerunnerPath,
                codeFile,
                f"--project={GEEproject}",
            ]
        )
        startProcess(loc, "01Prep", setup)

    print(f"01 Export prep for {loc} in {locGroup} started.")


def tryExportMarketShape(
    locGroup,
    loc,
    freqList,
    freqListStr,
    GEEbucket,
    country,
    shapeCodeFile,
    GEEproject,
    setup,
    setupImg,
    focalMinRadius,
):
    # runs logical checks to see if market shape export needs to be run for a given loc and locGroup,
    # runs the export if necessary, and updates the location file accordingly.

    # initialize google storage client
    client = storage.Client()

    # if marked complete or failed, exit
    if checkProcessStatus(loc, "02Map", setup) in ["complete", "failed"]:
        logger.debug(
            f"02 Map export already concluded for {loc} in {locGroup} -- skipping."
        )
        return

    # if export prep task not yest complete, exit
    if checkProcessStatus(loc, "01Prep", setupImg) != "complete":
        logger.debug(
            f"02 Map export for {loc} in {locGroup} cannot start since 01 export prep not complete -- skipping."
        )
        return

    # check if output already exists in GCS and GEE -- if so, exit and update
    bucketFolder = ee.data.listAssets(f"projects/{GEEbucket}/assets/PS_imgs")["assets"][
        0
    ]["id"].split("/")[-1]
    commandString = (
        f'earthengine ls "projects/{GEEbucket}/assets/PS_imgs/{bucketFolder}/{loc}proc"'
    )
    storageOutput = str(
        list(client.list_blobs("exports-mai2023", prefix=f"{locGroup}/shapes/"))
    )

    if (
        f"shp_{setup}" in str(subprocess.check_output([commandString], shell=True))
        and f"shp_{setup}_{locGroup}{loc}" in storageOutput
    ):
        geePath = f"projects/{GEEbucket}/assets/PS_imgs/{bucketFolder}/{loc}proc/shp_{setup}_{locGroup}{loc}"
        minRank = (
            ee.FeatureCollection(geePath).aggregate_min("strictnessRank").getInfo()
        )
        updateProcess(loc, "02Map", setup, minRank, column="minStrictnessRank")
        logger.debug(
            f"Updating minStrictnessRank for {loc}, setup {setup} in {GEEbucket}, {locGroup} to {minRank}"
        )

        if checkProcessStatus(loc, "02Map", setup):
            updateProcess(loc, "02Map", setup, "complete")
        else:
            startProcess(loc, "02Map", setup=setup)
            updateProcess(loc, "02Map", setup, "complete")

        print(
            f"02 Map export succeeded for {loc} in {locGroup} -- updating location file."
        )
        return

    # check if task is already running -- if so, exit
    if running_tasks(GEEproject, search_strings = [f"as2_{loc}", f"shp_{loc}"]):
        logger.debug(f"02 Map export currently running for {loc} in {locGroup}.")
        print(f"02 Map export currently running for {loc} in {locGroup}.")
        return

    # set up and modify GEE code to prepare for running
    codeFile = f"./temp/_findMktsB_{loc}.js"
    with open(f"./MAI/latest/{shapeCodeFile}", "r") as fin:
        with open(codeFile, "w") as fout:
            for line in fin:
                fout.write(
                    line.replace("INSERT_LOC_GROUP_HERE", locGroup)
                    .replace('"INSERT_LOC_HERE"', loc)
                    .replace('"INSERT_FREQ_LIST_HERE"', str(freqList))
                    .replace("INSERT_FREQ_DAY_STR_HERE", freqListStr)
                    .replace("INSERT_BUCKET_HERE", GEEbucket)
                    .replace("INSERT_COUNTRY_HERE", country.replace(" ", ""))
                    .replace("INSERT_FOCAL_MIN_RADIUS_HERE", str(focalMinRadius))
                )

    # run the GEE code and update the location file
    subprocess.call(
        [
            nodePath,
            "--no-deprecation",
            eerunnerPath,
            codeFile,
            f"--project={GEEproject}",
        ]
    )
    startProcess(loc, "02Map", setup=setup)
    print(f"02 Map export for {loc} in {locGroup} started.")


def tryActivityPrep(
    locGroup,
    loc,
    country,
    GEEbucket,
    activityPrepCodeFile,
    GEEproject,
    setup,
    setupMap,
    signalCutoff,
):
    # runs logical checks to see if market activity export needs to be run for a given loc and locGroup,
    # runs the export if necessary, and updates the location file accordingly.

    # initialize google storage client
    client = storage.Client()
    bucketFolder = ee.data.listAssets(f"projects/{GEEbucket}/assets/PS_imgs")["assets"][
        0
    ]["id"].split("/")[-1]
    # if marked complete or failed, exit
    if checkProcessStatus(loc, "03ActivityPrep", setup) in ["complete", "failed"]:
        logger.debug(
            f"03 Activity prep already concluded for {loc} in {locGroup} -- skipping."
        )
        return

    # if mapping asset from previous step not present, exit
    if checkProcessStatus(loc, "02Map", setupMap) != "complete":
        logger.debug(f"02 Map export not complete for {loc} in {locGroup} -- skipping.")
        return

    if (
        checkProcessStatus(loc, "02Map", setupMap) == "complete"
        and checkProcessStatus(loc, "02Map", setupMap, column="minStrictnessRank")
        == None
    ):
        geePath = f"projects/{GEEbucket}/assets/PS_imgs/{bucketFolder}/{loc}proc/shp_{setupMap}_{locGroup}{loc}"
        minRank = (
            ee.FeatureCollection(geePath).aggregate_min("strictnessRank").getInfo()
        )
        updateProcess(loc, "02Map", setupMap, minRank, column="minStrictnessRank")
        logger.debug(
            f"Updating minStrictnessRank for {loc}, setup {setupMap} in {GEEbucket}, {locGroup} to {minRank}"
        )

        if checkProcessStatus(loc, "02Map", setupMap):
            updateProcess(loc, "02Map", setupMap, "complete")
        else:
            startProcess(loc, "02Map", setupMap=setup)
            updateProcess(loc, "02Map", setupMap, "complete")

    # if completed map export has too low of a maximum value
    if (
        checkProcessStatus(loc, "02Map", setupMap, column="minStrictnessRank")
        > signalCutoff
        and checkProcessStatus(loc, "02Map", setupMap, column="runAnyway") != "yes"
    ):
        logger.debug(
            f"02 Map export peak too low for {loc} in {locGroup} -- removing from processing pipeline."
        )
        return

    # check if output already exists in GEE -- if so, exit and update
    commandString = (
        f'earthengine ls "projects/{GEEbucket}/assets/PS_imgs/{bucketFolder}/{loc}proc"'
    )

    if f"exp{setup}" in str(subprocess.check_output([commandString], shell=True)):
        if checkProcessStatus(loc, "03ActivityPrep", setup):
            updateProcess(loc, "03ActivityPrep", setup, "complete")
        else:
            startProcess(loc, "03ActivityPrep", setup=setup)
            updateProcess(loc, "03ActivityPrep", setup, "complete")
        print(
            f"03 Activity prep succeeded for {loc} in {locGroup} -- updating location file."
        )
        return

    # check if task is already running -- if so, exit
    if running_tasks(GEEproject, search_strings = [f"actPrep_{loc}"]):
        logger.debug(f"03 Activity prep currently running for {loc} in {locGroup}.")
        return

    # REMOVE
    # if True:
    #    print(f'Task for {loc} in {locGroup} would fail with new setup. Assign to failed.')
    #    startProcess(loc, '03ActivityPrep', setup = setup)
    #    updateProcess(loc, '03ActivityPrep', setup, 'failed', fail_reason = "newSetup")
    #    return

    # set up and modify GEE code to prepare for running
    if checkLocationFileStatus(loc, "stored_in_gcs") == 0:
        codeFile = f"./temp/_activityPrep_{loc}.js"
        with open(f"./MAI/latest/{activityPrepCodeFile}", "r") as fin:
            with open(codeFile, "w") as fout:
                for line in fin:
                    fout.write(
                        line.replace("INSERT_LOC_GROUP_HERE", locGroup)
                        .replace('"INSERT_LOC_HERE"', loc)
                        .replace("INSERT_BUCKET_HERE", GEEbucket)
                        .replace("INSERT_COUNTRY_HERE", country.replace(" ", ""))
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
            if any(s.split("/")[-1].startswith(str(year)) for year in range(2017, 2022))
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
        bucketFolder = ee.data.listAssets(f"projects/{GEEbucket}/assets/PS_imgs")[
            "assets"
        ][0]["id"].split("/")[-1]
        geePath = f"projects/{GEEbucket}/assets/PS_imgs/{bucketFolder}/{loc}proc/shp_{setupMap}_{locGroup}{loc}"
        fc = ee.FeatureCollection(geePath).filter(ee.Filter.lt("strictnessRank", 25))
        weekday_list = list(set(fc.aggregate_array("weekdayShp").getInfo()))

        def extract_weekday(file_path):
            date_string = file_path.split("/")[-1][0:8]
            date_obj = datetime.strptime(date_string, "%Y%m%d")
            return (
                date_obj.weekday() + 1
            ) % 7  # add 1 and mod 7 to get Sunday=0 instead of Monday=0

        harmonized_files_md = [
            file for file in harmonized_files if extract_weekday(file) in weekday_list
        ]
        harmonized_files_non_md = [
            file for file in harmonized_files if file not in harmonized_files_md
        ]

        #if len(harmonized_files_md) >2000:
        #    sample_size = min(
        #        2000 - len(harmonized_files_md), len(harmonized_files_non_md)
        #    )
        #    harmonized_files_non_md = random.sample(
        #        harmonized_files_non_md, sample_size
        #    )

        total_files = len(harmonized_files_md) + len(harmonized_files_non_md)
        
        if total_files > 1900:
            # Keep all market day files, sample non-market day files to reach 2500 total
            max_non_md_files = 1900 - len(harmonized_files_md)
            if max_non_md_files > 0:
                harmonized_files_non_md = random.sample(harmonized_files_non_md, min(max_non_md_files, len(harmonized_files_non_md)))
            else:
                harmonized_files_non_md = []  # No room for non-market day files
                
        harmonized_files = harmonized_files_md + harmonized_files_non_md
        #### END FILTER BY WEEKDAY

        masks_files = extract_masks_files(f"gs://ps-imgs-mai1/{loc}", loc)
        masks_files = [
            s
            for s in masks_files
            if any(s.split("/")[-1].startswith(str(year)) for year in range(2017, 2022))
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
            
        harmonized_files = filter_list1_by_list2(harmonized_files, masks_files)
                
        logger.debug("masks_files: %d, %s", len(masks_files), masks_files[0:5])

        # Process JSON files and create FeatureCollection
        fs = gcsfs.GCSFileSystem()
        with fs.open(f"gs://ps-imgs-mai1/imgProperties/{loc}.geojson") as f:
            geojson = gpd.read_file(f)

        exclude_list = [
            "2026",
            "2025",
            "2024",
            "2023",
            "2022",
            "2016",
            "201701",
            "201702",
            "201703",
            "201704",
        ]

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

        #print('ImageIDs:', image_ids[0:3])
        #print('harmonized_files:', len(harmonized_files), harmonized_files[0:3])
        #print('masks_files:', masks_files[0:10])
        
        harmonized_files = filter_matching_files(image_ids, harmonized_files)
        #print('harmonized_files:', len(harmonized_files), harmonized_files[0:3])

        # Convert GeoJSON to JavaScript code
        js_code = geojson_to_ee_featurecollection(geojson_dict)
        logger.debug("Generated JS code")
        # print(harmonized_files[0:5])

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
        codeFile = f"./temp/_activityPrep_{loc}.js"
        with open(f"./MAI/latest/{activityPrepCodeFile}_gcs", "r") as fin:
            with open(codeFile, "w") as fout:
                for line in fin:
                    fout.write(
                        line.replace("INSERT_LOC_GROUP_HERE", locGroup)
                        .replace('"INSERT_LOC_HERE"', loc)
                        .replace("INSERT_BUCKET_HERE", GEEbucket)
                        .replace("INSERT_COUNTRY_HERE", country.replace(" ", ""))
                        .replace("ENTERNODEHERE", nodePath.replace("bin", "lib"))
                    )

    # run the GEE code and update the location file
    subprocess.call(
        [
            nodePath,
            "--no-deprecation",
            eerunnerPath,
            codeFile,
            f"--project={GEEproject}",
        ]
    )
    startProcess(loc, "03ActivityPrep", setup=setup)
    print(f"03 Activity prep for {loc} in {locGroup} started.")


def tryExportMarketActivity(
    locGroup,
    loc,
    country,
    GEEbucket,
    activityExportCodeFile,
    GEEproject,
    setup,
    setupMap,
    setupActivityPrep,
):
    # runs logical checks to see if market activity export needs to be run for a given loc and locGroup,
    # runs the export if necessary, and updates the location file accordingly.

    # initialize google storage client
    client = storage.Client()

    # if marked complete or failed, exit
    if checkProcessStatus(loc, "04ActivityExport", setup) in ["complete", "failed"]:
        logger.debug(
            f"04 Activity export already concluded for {loc} in {locGroup} -- skipping."
        )
        return

    # if mapping asset from previous step not present, exit
    if checkProcessStatus(loc, "03ActivityPrep", setupActivityPrep) != "complete":
        logger.debug(
            f"03 Activity prep not complete for {loc} in {locGroup} -- skipping."
        )
        return

    if checkProcessStatus(loc, "02Map", setupMap) != "complete":
        logger.debug(f"02 Map not complete for {loc} in {locGroup} -- skipping.")
        return

    # check if output already exists in GCS -- if so, exit and update
    storageOutput = str(
        list(client.list_blobs("exports-mai2023", prefix=f"{locGroup}/measures/"))
    )

    if f"{setup}_maxpMax{loc}" in storageOutput:
        if checkProcessStatus(loc, "04ActivityExport", setup):
            updateProcess(loc, "04ActivityExport", setup, "complete")
        else:
            startProcess(loc, "04ActivityExport", setup)
            updateProcess(loc, "04ActivityExport", setup, "complete")

        print(
            f"04 Activity export succeeded for {loc} in {locGroup} -- updating location file."
        )
        if checkLocationFileStatus(loc, "stored_in_gcs") == 1:
            updateLocationFileStatus(
                loc, "lastImageUpdate", "2021-01-26 08:03:52", replace=True
            )
            updateLocationFileStatus(
                loc, "lastActivityUpdate", "2021-01-26 08:03:52", replace=True
            )
        return

    # check if task is already running -- if so, exit
    if running_tasks(GEEproject, search_strings = [setup, loc]):
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
                    .replace("INSERT_COUNTRY_HERE", country.replace(" ", ""))
                )

    # run the GEE code and update the location file
    subprocess.call(
        [
            nodePath,
            "--no-deprecation",
            eerunnerPath,
            codeFile,
            f"--project={GEEproject}",
        ]
    )
    startProcess(loc, "04ActivityExport", setup=setup)
    print(f"04 Activity export for {loc} in {locGroup} started.")


# -------------------------------------------------------------------------------------------------------------------------------
# ACTIVITY FUNCTIONS
# -------------------------------------------------------------------------------------------------------------------------------


def activityRunner(name, signalCutoff):
    print("activityRunner started...")

    while True:
        cnx = mysql.connector.connect(
            user="root",
            password="BMkjM8_)-tN8R33u",
            host="34.72.234.161",
            database="mai-database",
        )

        query = f"""
        SELECT l.Location
        FROM `mai-database`.`location_file` l
        WHERE l.Assignment = '{name}'
        AND l.to_delete IS NULL
        AND NOT EXISTS (
            SELECT 1
            FROM `mai-database`.`activity_market` a
            WHERE a.Location = l.Location
        ) 
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
            FROM `mai-database`.`process_runs` pr
            WHERE pr.Location = l.Location
            AND pr.Process = '02Map'
            AND pr.Setup = 'MpM6'
            AND pr.Status = 'complete'
            AND (pr.minStrictnessRank <= {signalCutoff} OR pr.runAnyway="yes")
        ) AND EXISTS (
            SELECT 1
            FROM `mai-database`.`process_runs` pr
            WHERE pr.Location = l.Location
            AND pr.Process = 'PropExport'
            AND pr.Status = 'complete'
        ) AND NOT EXISTS (
            SELECT 1
            FROM `mai-database`.`process_runs` pr
            WHERE pr.Location = l.Location
            AND pr.Process = 'activityUpload'
            AND pr.Status = 'failed'
            AND pr.FailReason = 'not enough SD observations for normalization'
        )
        """
#         print('activity query')
#         print(query)

        cursor = cnx.cursor()
        cursor.execute(query)
        response = cursor.fetchall()
        locs = [row[0] for row in response]

        cursor.close()

        if locs:
            print(f"locs for activityUpload: {locs}")
            activityUploader(locs)

        # Delete false positives
        query = """
            SELECT Location FROM location_file lf
            WHERE to_delete = "yes"
            AND 00DownStatus IN ('complete', 'initiated', 'updating', 'failed')
            AND NOT EXISTS (
                SELECT 1
                FROM process_runs pr 
                WHERE lf.Location=pr.Location
                AND Setup ="MpM6"
                AND Status= "deleted"
            )
        """
        #
        if name == "tillmann":
            locs = list(pd.read_sql(query, con=engine)["Location"])
            print("start deleting ", locs[0:10], len(locs))
            parallel_process_locations(locs, delete_false_positives)
            print("done deleting ", locs[0:10], len(locs))

            # Delete duplicate images
            cursor = cnx.cursor()
            cursor.execute(
                "SELECT Location FROM location_file WHERE (lastImageDedupe < '2024-10-30' OR lastImageDedupe IS NULL)"
            )
            results = cursor.fetchall()
            locs = [result[0] for result in results]
            for loc in locs:
                delete_duplicates(loc)
            cursor.close()

        cnx.close()

        time.sleep(120)


# -------------------------------------------------------------------------------------------------------------------------------
# UTILITY FUNCTIONS
# -------------------------------------------------------------------------------------------------------------------------------

def running_tasks(GEEproject, search_strings = []):
    
    ee.data.setCloudApiUserProject(GEEproject)
    all_tasks = [task['metadata'] for task in ee.data.listOperations()]
    running_tasks = [task for task in all_tasks if task['state'] in ['PENDING', 'RUNNING']]
    if search_strings:
        running_tasks = [task for task in running_tasks if any(s in task['description'] for s in search_strings)]
        
    return running_tasks


def format_acquired_date(value):
            """Ensure 'acquired' dates are consistently formatted as ISO 8601 (YYYY-MM-DDTHH:MM:SS.ssssss+00:00)."""
            if isinstance(value, datetime):
                return value.strftime("%Y-%m-%dT%H:%M:%S.%f%z")

            if isinstance(value, str):
                # Fix non-standard timezone "+00" → "+00:00"
                if value.endswith("+00"):
                    value += ":00"

                # Fix invalid second value ":60" → ":59.999999"
                if ":60." in value:
                    value = value.replace(":60.", ":59.9")

                # Define multiple date formats
                formats = [
                    "%Y/%m/%d %H:%M:%S.%f%z",  # "2020/12/09 07:27:09.991+00"
                    "%Y/%m/%d %H:%M:%S%z",  # "2020/06/11 07:25:53+00:00"
                    "%Y-%m-%dT%H:%M:%S.%f%z",  # ISO 8601
                    "%Y/%m/%d %H:%M:%S.%f",  # Missing timezone
                    "%Y/%m/%d %H:%M:%S",  # No milliseconds, no timezone
                ]

                # Try parsing with each format
                for fmt in formats:
                    try:
                        dt = datetime.strptime(value, fmt)
                        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
                    except ValueError:
                        continue

            return value  # Return unchanged if format is unrecognized


def processFromGEEDescription(GEEDescription, loc=None, setup=False):
    key = GEEDescription.split("  ")[2].split("_")[0].split("lon")[0]
    lookup = {
        "prop": "PropExport",
        "diffImg": "01Prep",
        "shp": "02Map",
        "as2": "02Map",
        "actPrep": "03ActivityPrep",
        "exportAct5": "04ActivityExport",
    }
    process = lookup[key]
    
    if loc and setup:
        processDF = allProcessRecords(loc)
        subset = processDF[processDF["Process"] == process]

        if subset.empty:
            print(f"No matching process found for '{process}' for {loc}. ")
            print(f"Available values are: {processDF['Process'].unique()}")
            setup=""
        else:
            setup = subset["Setup"].iloc[0]

        return process, setup
    else:
        return process, ""


def list_blobs_with_prefix(bucket_name, prefix, storage_client, delimiter=None):
    # lists all images in a specific GCS folder. We use it as an input into the following function, identifying images that we no longer need to download.

    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)
    return blobs


def remove_overlapping_strings(list1, list2):
    # checks for each of the strings in list1 whether it forms at least part of any of the strings in list2. We use it to filter out already downloaded imagery from the new set of tasks

    return [string for string in list1 if string not in "|".join(list2)]


def saveConvexHull(loc):
    storage_client = storage.Client()
    bucketName = "mai_2023"
    bucket = storage_client.get_bucket(bucketName)
    country = checkLocationFileStatus(loc, "country")
    countryCode = checkLocationFileStatus(loc, "locGroup").split("_")[0]

    # pull a list of gcloud storage buckets, and find the names of the json outputs
    output = str(
        subprocess.check_output([f"gcloud storage ls gs://mai_2023/**"], shell=True)
    )
    jsonIDs = set(re.findall(rf"gs://mai_2023/((\d+_)?[A-Z]\w+(_\d+)?)/", output))
    jsonIDs = [jsonID[0] for jsonID in jsonIDs]
    jsonIDs = [
        ID
        for ID in list(jsonIDs)
        if (country in ID or (countryCode in ID and countryCode != ""))
    ]

    for jsonID in jsonIDs:
        try:
            data_string = bucket.get_blob(
                f"{jsonID}/{jsonID}.geojson"
            ).download_as_string()
        except:
            continue

        data = json.loads(data_string)
        for j in data["features"]:
            mktID = j["properties"]["mktID"].replace(
                ".", "_", 2
            )  # fix name format of mktID

            if mktID == loc:  # if this is the location we are looking for
                dumped = json.dumps(j)  # dump feature json into string format
                added = (
                    '{"type": "FeatureCollection", "features": [{'
                    + dumped[1:-1]
                    + "}]}"
                )  # add type classification to the string

                # Create a GeoJSON string and load it as a Shapely geometry
                geometry = geojson.loads(added)
                geometry_type = geometry["features"][0]["geometry"]["type"]
                shapely_geometry = shape(geometry["features"][0]["geometry"])

                if geometry_type == "MultiPolygon":
                    # Compute the convex hull for a MultiPolygon
                    convex_hull = shapely_geometry.convex_hull
                    convex_hull_geometry_type = "Polygon"
                else:
                    # Compute the convex hull for a Polygon
                    convex_hull = shapely_geometry
                    convex_hull_geometry_type = "Polygon"

                # Convert the convex hull to GeoJSON
                convex_hull_geojson = geojson.Feature(
                    geometry=convex_hull, properties={}
                )

                # Save the convex hull as a GeoJSON file
                with open(f"./temp/Jsons/{loc}feature.geojson", "w") as file:
                    geojson.dump(convex_hull_geojson, file)

                if os.path.isfile(f"./temp/Jsons/{loc}feature.geojson"):
                    logger.debug(f"GeoJSON saved successfully for {loc}")
                else:
                    print(f"WARNING: GeoJSON failed to save for {loc}...")

                return
    updateLocationFileStatus(loc, "00DownStatus", "failed", replace=True)
    updateLocationFileStatus(loc, "00aDownNoSRStatus", "failed", replace=True)
    updateLocationFileStatus(loc, "notes", "missingGeoJSON", replace=True)
    raise Exception(
        f"GeoJSON not found for {loc}. Check that it is uploaded to google storage correctly"
    )


def deleteDuplicates(loc):
    # Delete duplicate image assets within collections##

    unique_matches = set()
    GEEbucket = checkLocationFileStatus(loc, "bucket")

    bucketFolder = ee.data.listAssets(f"projects/{GEEbucket}/assets/PS_imgs")["assets"][0][
        "id"
    ].split("/")[-1]
    root_name = f"projects/{GEEbucket}/assets/PS_imgs/{bucketFolder}/"
    folder_name = root_name + loc
    asset_list = ee.data.getList({"id": folder_name})
    id_list = [item["id"] for item in asset_list]  ##

    for asset in id_list:
        assetID = asset.replace(folder_name, "").replace("/", "")
        match = re.search(r"^(.*?)_Analytic", assetID)
        if match:
            match_str = match.group(1)
            unique_matches.add(match_str)

    duplicates = [
        match
        for match in list(unique_matches)
        if sum(1 for id_item in id_list if match in id_item) > 1
    ]
    # Print the duplicates
    logger.debug(
        f"Duplicates found in {loc} of the string before '_Analytic': {len(duplicates)} {loc}"
    )
    for duplicate in sorted(duplicates):
        # list all assets with this ID
        result = [item for item in id_list if duplicate in item]
        for res in result[1:]:  # delete the second result and onwards
            ee.data.deleteAsset(res)

    updateLocationFileStatus(
        loc, "lastImageDedupe", datetime.today().strftime("%Y-%m-%d"), replace=True
    )

    return


# Configure logger
logger = logging.getLogger(__name__)


def deleteDuplicates_gcs(loc):
    """
    List all file names in a specific GCS folder (excluding paths), including subfolders.
    If a file name is already in the list, delete the duplicate file from GCS.

    Parameters:
    bucket_name (str): The name of the GCS bucket containing the objects.
    folder_prefix (str): The prefix (folder path) to search for files.
    """
    bucket_name = "ps-imgs-mai1"
    try:
        # Initialize the GCS client
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # Set to track unique file names
        unique_file_names = set()

        # List blobs within the specified folder
        blobs = client.list_blobs(bucket_name, prefix=loc)

        for blob in blobs:
            try:
                # Extract the file name (exclude path)
                file_name = blob.name.split("/")[-1]

                if file_name in unique_file_names:
                    # Duplicate detected, delete the blob
                    logger.debug(f"Duplicate file detected and deleted: {blob.name}")
                    blob.delete()
                else:
                    # Add the file name to the set
                    unique_file_names.add(file_name)

            except Exception as e:
                logger.error(f"Failed to process object {blob.name}: {e}")

        logger.debug(
            f"Processed {len(unique_file_names)} unique files in folder {loc} of bucket {bucket_name}"
        )

    except Exception as e:
        logger.error(f"Error processing folder {loc} in bucket {bucket_name}: {e}")


# Function to get the list of order folders in the GCS bucket
def get_order_folders(bucket_path):
    result = (
        subprocess.check_output(f"gsutil ls {bucket_path} | grep '/$'", shell=True)
        .decode("utf-8")
        .splitlines()
    )
    return [s.replace(bucket_path, "").replace("/", "") for s in result if s.strip()]


# Function to move files from the PSScene subfolder
def check_subfolder_exists(bucket_path, subfolder_name):
    try:
        result = (
            subprocess.check_output(["gsutil", "ls", "-d", bucket_path])
            .decode("utf-8")
            .splitlines()
        )
        subfolder_path = f"{bucket_path.rstrip('/')}/{subfolder_name}/"
        return subfolder_path in result
    except subprocess.CalledProcessError as e:
        print(f"Error checking subfolder: {e}")
        return False


def rename_blob(bucket, blob_name, destination_name):
    try:
        blob = bucket.blob(blob_name)
        bucket.rename_blob(blob, destination_name)
        return blob_name
    except Exception as e:
        raise RuntimeError(f"Failed to move {blob_name} to {destination_name}: {e}")


def consolidate_gcs_files(loc):
    # Initialize the client and specify the bucket and primary directory
    client = storage.Client()
    bucket_name = "ps-imgs-mai1"
    primary_dir = f"{loc}/"

    bucket = client.bucket(bucket_name)

    # List all blobs that are within subdirectories of the primary directory
    blobs = bucket.list_blobs(prefix=primary_dir)
    blobs_to_move = [
        blob.name for blob in blobs if "/" in blob.name[len(primary_dir) :]
    ]

    total_files = len(blobs_to_move)
    if total_files == 0:
        print(f"No files to move for {loc}.")
        return

    print(f"Total GCS files to consolidate for {loc}: {total_files}")

    # Initialize the progress bar
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all move operations to the executor
        future_to_blob = {
            executor.submit(
                rename_blob, bucket, blob_name, primary_dir + blob_name.split("/")[-1]
            ): blob_name
            for blob_name in blobs_to_move
        }

        # Initialize tqdm progress bar
        with tqdm(total=total_files, desc="Moving files", unit="file") as pbar:
            for future in as_completed(future_to_blob):
                blob_name = future_to_blob[future]
                try:
                    moved_blob = future.result()
                    pbar.update(1)
                except Exception as e:
                    print(f"\nError moving {blob_name}: {e}")
                    pbar.update(1)  # Update progress even if there's an error


def rename_file(old_file, new_file):
    try:
        subprocess.run(
            ["gcloud", "storage", "mv", old_file, new_file],
            stdout=subprocess.DEVNULL,  # Suppress standard output
            stderr=subprocess.PIPE,  # Capture standard error
            check=True,  # Raise exception on failure
        )
        # subprocess.run(['gsutil', 'mv', old_file, new_file], check=True)
        # print(f"Renamed: {old_file} -> {new_file}")
    except subprocess.CalledProcessError as e:
        print(f"Error renaming {old_file}: {e}")


def rename_files(loc, to_match, to_replace, to_fillin):
    """
    Renames files in the specified GCS bucket that match the given pattern by replacing a substring.

    Args:
        bucket_name (str): Name of the GCS bucket.
        file_pattern (str): Prefix pattern to filter files (supports prefix matching).
        to_replace (str): Substring to replace in the file names.
        to_fillin (str): Substring to replace with in the file names.
    """
    client = storage.Client()
    bucket = client.bucket("ps-imgs-mai1")

    # List all blobs within the main folder
    blobs = bucket.list_blobs(prefix=f"{loc}/")

    # Filter blobs that end with the 'to_match' string
    files_to_move = []
    for blob in blobs:
        if blob.name.endswith(to_match):
            if to_replace in blob.name:
                new_name = blob.name.replace(to_replace, to_fillin)
                files_to_move.append((blob.name, new_name))

    print(
        f"Renaming {len(files_to_move)} images in {loc}: '{to_replace}' --> '{to_fillin}'"
    )

    if files_to_move:
        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=3) as executor:
            # Submit all rename tasks to the executor
            futures = [
                executor.submit(rename_blob, bucket, old_name, new_name)
                for old_name, new_name in files_to_move
            ]

            # Display progress using tqdm
            with tqdm(total=len(futures), desc="Renaming files", unit="file") as pbar:
                for future in as_completed(futures):
                    # You can handle results or exceptions here if needed
                    pbar.update(1)
    else:
        print(f"No files found with the pattern '{to_match}' in bucket '{loc}'.")


# Main function to execute the process
def prepare_imagery_gcs(loc):
    logger.debug(f"Processing images folder: {loc}")

    # Move all files into the main folder
    consolidate_gcs_files(loc)

    # Rename files (rename masks if they include the substring 'coreg')
    rename_files(loc, "udm2_clip_file_format.tif", "_coreg", "")
    try:
        rename_files(
            loc,
            "AnalyticMS_SR_harmonized_coreg_clip_file_format.tif",
            "AnalyticMS_SR_harmonized_coreg_clip_file_format.tif",
            "SR_coreg_clip_file_format.tif",
        )
    except Exception as e:
        print("error:", e)

    try:
        rename_files(
            loc,
            "AnalyticMS_SR_harmonized_clip_file_format.tif",
            "AnalyticMS_SR_harmonized_clip_file_format.tif",
            "SR_clip_file_format.tif",
        )
    except Exception as e:
        print("error:", e)

    print("Imagery prepared for processing", loc)
    return


# Function to extract harmonized files and image IDs
def extract_harmonized_files_and_ids(GCS_BUCKET, loc):
    # print('GCS bucket', GCS_BUCKET)
    cloud_files = (
        subprocess.check_output(["gsutil", "ls", "-r", GCS_BUCKET])
        .decode("utf-8")
        .splitlines()
    )
    # print('cloud_files',cloud_files)
    harmonized_files = [
        file for file in cloud_files if "_SR_" in file and file.endswith(".tif")
    ]
    harmonized_files = [
        file.replace(f"gs://ps-imgs-mai1/{loc}/", "").replace(
            "_clip_file_format.tif", ""
        )
        for file in harmonized_files
    ]
    # Extract image IDs from harmonized files
    pattern1 = f"\/(20.*?)_3B_"
    pattern2 = f"^(20.*?)_3B_"
    image_IDs = list(
        set(
            [
                match.group(1)
                if (match := re.search(pattern1, text))
                or (match := re.search(pattern2, text))
                else None
                for text in harmonized_files
            ]
        )
    )

    return harmonized_files, image_IDs


def extract_masks_files(GCS_BUCKET, loc):
    # print('GCS bucket', GCS_BUCKET)
    cloud_files = (
        subprocess.check_output(["gsutil", "ls", "-r", GCS_BUCKET])
        .decode("utf-8")
        .splitlines()
    )
    masks_files = [
        file for file in cloud_files if "_udm" in file and file.endswith(".tif")
    ]
    masks_files = [
        file.replace(f"gs://ps-imgs-mai1/{loc}/", "").replace(
            "_clip_file_format.tif", ""
        )
        for file in masks_files
    ]
    # Extract image IDs from harmonized files

    return masks_files


def process_blob(blob_name, image_IDs, imageBucket):
    try:
        image_id = [image_id for image_id in image_IDs if image_id in blob_name][0]
        blob = imageBucket.blob(blob_name)
        json_content = blob.download_as_text()
        json_data = json.loads(json_content)

        # Filter properties
        filtered_properties = {
            k: v
            for k, v in json_data.get("properties", {}).items()
            if k in allowed_properties
        }
        filtered_properties["image_ID"] = image_id
        json_data["properties"] = filtered_properties

        # Create Earth Engine Feature
        geometry = json_data.get("geometry", None)
        properties = json_data["properties"]
        feature = ee.Feature(ee.Geometry(geometry) if geometry else None, properties)

        return json_data, feature  # Or return whatever you need
    except Exception as e:
        print(f"Error processing blob {blob_name}: {e}")
        return None, None


def process_json_files(loc, MAX_WORKERS=10):
    _, image_IDs = extract_harmonized_files_and_ids(f"gs://ps-imgs-mai1/{loc}", loc)
    # print('image_IDs',image_IDs)
    client = storage.Client()
    imageBucket = client.bucket("ps-imgs-mai1")
    blobs = list(imageBucket.list_blobs(prefix=f"{loc}"))
    # print('blobs',blobs)
    # filter blobs that end with '_metadata.json'
    filenames = [f"{image_id}_metadata.json" for image_id in image_IDs]
    metadata_blobs = [
        blob.name for blob in blobs if blob.name.split("/")[-1] in filenames
    ]
    # print('metadata_blobs',metadata_blobs)
    print(f"Processing {len(metadata_blobs)} metadata jsons for {loc}.")

    features = []
    features_json = []

    # I/O-bound operations
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # submit all tasks
        future_to_blob = {
            executor.submit(process_blob, blob_name, image_IDs, imageBucket): blob_name
            for blob_name in metadata_blobs
        }

        # as each task completes, collect the result
        for future in as_completed(future_to_blob):
            blob_name = future_to_blob[future]
            try:
                json_data, feature = future.result()
                if json_data:
                    features_json.append(json_data)
                if feature:
                    features.append(features)
            except Exception as exc:
                print(f"{blob_name} generated an exception: {exc}")

    return features_json, features


# Function to create the GeoJSON structure
def create_geojson(features_json):
    return {
        "type": "FeatureCollection",
        "columns": {
            "acquired": "String",
            "anomalous_pixels": "Integer",
            "clear_confidence_percent": "Integer",
            "clear_percent": "Integer",
            "cloud_percent": "Integer",
            "gsd": "Float",
            "heavy_haze_percent": "Integer",
            "instrument": "String",
            "light_haze_percent": "Integer",
            "satellite_azimuth": "Float",
            "satellite_id": "String",
            "view_angle": "Float",
            "visible_confidence_percent": "Integer",
            "visible_percent": "Integer",
        },
        "features": features_json,
    }


# Function to convert GeoJSON to JavaScript FeatureCollection string
def geojson_to_ee_featurecollection(geojson):
    features_js = []
    for feature in geojson["features"]:
        geometry = feature["geometry"]
        properties = feature["properties"]

        geom_js = f"null" if geometry else "null"

        # for item in properties:
        #    if "acquired" in item:
        #        try:
        #            # Try parsing 'YYYY/MM/DD HH:MM:SS.sss+ZZ' format
        #            dt = datetime.strptime(item["acquired"], "%Y/%m/%d %H:%M:%S.%f%z")
        #        except ValueError:
        #            try:
        #                # Try parsing 'YYYY-MM-DDTHH:MM:SS.ssssss+ZZ:ZZ' format
        #                dt = datetime.strptime(item["acquired"], "%Y-%m-%dT%H:%M:%S.%f%z")
        #            except ValueError:
        #                continue  # Skip if both formats fail
        #
        #        # Convert to ISO format
        #        item["acquired"] = dt.strftime("%Y-%m-%dT%H:%M:%S.%f%z")

        props_js = json.dumps(properties, indent=2)

        feature_js = f"ee.Feature(\n    {geom_js},\n    {props_js})"
        features_js.append(feature_js)

    return (
        "var properties = ee.FeatureCollection([\n  "
        + ",\n  ".join(features_js)
        + "\n]);"
    )


# -------------------------------------------------------------------------------------------------------------------------------
# API REQUEST PARAMETERS
# -------------------------------------------------------------------------------------------------------------------------------


def fn_search_para_1():
    search_para_1 = {
        "item_types": ["PSScene"],
        "filter": {
            "type": "AndFilter",
            "config": [
                {
                    "type": "GeometryFilter",
                    "field_name": "geometry",
                    "config": {
                        "type": "Polygon",
                        "coordinates": None,  # Will be updated for each file
                    },
                },
                {
                    "type": "DateRangeFilter",
                    "field_name": "acquired",
                    "config": {"gte": None, "lte": None},
                },
                {
                    "type": "RangeFilter",
                    "field_name": "cloud_cover",
                    "config": {"lte": None},
                },
                {
                    "type": "RangeFilter",
                    "field_name": "anomalous_pixels",
                    "config": {"lte": None},
                },
                {
                    "type": "RangeFilter",
                    "field_name": "clear_confidence_percent",
                    "config": {"gte": None},
                },
                {
                    "type": "RangeFilter",
                    "field_name": "clear_percent",
                    "config": {"gte": None},
                },
                {
                    "type": "StringInFilter",
                    "field_name": "ground_control",
                    "config": None,
                },
                {"type": "AssetFilter", "config": ["ortho_analytic_4b_sr"]},
                {"type": "AssetFilter", "config": ["ortho_udm2"]},
                {
                    "type": "PermissionFilter",
                    "config": ["assets.ortho_analytic_4b_sr:download"],
                },
            ],
        },
    }
    return search_para_1


# Data API search parameters
def fn_search_para_2():
    search_para_2 = {
        "item_types": ["PSScene"],
        "filter": {
            "type": "AndFilter",
            "config": [
                {
                    "type": "GeometryFilter",
                    "field_name": "geometry",
                    "config": {
                        "type": "Polygon",
                        "coordinates": None,  # Will be updated for each file
                    },
                },
                {
                    "type": "DateRangeFilter",
                    "field_name": "acquired",
                    "config": {"gte": None, "lte": None},
                },
                {
                    "type": "RangeFilter",
                    "field_name": "cloud_cover",
                    "config": {"lte": None},
                },
                {
                    "type": "AssetFilter",
                    "config": ["ortho_analytic_4b_sr", "ortho_analytic_4b"],
                },
                {"type": "AssetFilter", "config": ["ortho_udm2"]},  # ,
                # {
                #    "type":"PermissionFilter",
                #     "config":[
                #        "assets.ortho_analytic_4b_sr:download"
                #     ]
                # }
            ],
        },
    }
    return search_para_2


# https://developers.planet.com/docs/apis/data/searches-filtering/#stringinfilter
# Order API parameters
def fn_order_payload():
    order_payload = {
        "name": None,  #
        "order_type": "partial",  # deliver only those items for which all parts of bundle are available
        "products": [
            {
                "item_ids": None,  # to be filled in later
                "item_type": "PSScene",
                "product_bundle": "analytic_sr_udm2",  # https://developers.planet.com/apis/orders/product-bundles-reference/
            }
        ],
        "tools": [  # add or remove tools as needed
            {
                "clip": {
                    "aoi": None  #
                }
            },
            {"file_format": {"format": "COG"}},
            {"harmonize": {"target_sensor": "Sentinel-2"}},
            #{
            #    "coregister": {
            #        "anchor_item": None  # find the perfect image here (recent, little haze, little distortion)
            #    }
            #},  # ,
            # {
            #  "file_format:" : {
            #      "format": "COG"
            #  }
            # }
        ],
        # "delivery": {
        #    "google_cloud_storage": {
        #        "bucket": None,
        #        "credentials": None,
        #        "path_prefix": None # locGroup+"/loc"+loc+"/"
        #    }
        "delivery": {
            "google_cloud_storage": {
                "bucket": None,
                "path_prefix": None,
                "credentials": None,
            }
        },
    }
    return order_payload

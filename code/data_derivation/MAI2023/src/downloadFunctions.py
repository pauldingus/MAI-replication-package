import ee
import os
import re
import json
import time
import requests
from dotenv import load_dotenv
from google.cloud import storage

# from MAI2023.main import checkRunningOrders, saveConvexHull, logger, fn_search_para_1, search_url, retry_interval, remove_overlapping_strings, fn_search_para_2, deleteDuplicates, deleteDuplicates_gcs, extract_harmonized_files_and_ids, process_json_files, create_geojson, max_retries

from MAI2023.src.dbFunctions import checkLocationFileStatus, updateLocationFileStatus, assignForProcessing, updateUpdateProcess

load_dotenv(dotenv_path=os.path.expanduser("~/.env") if os.geteuid() != 0 else "/root/MAI2023/.env")

def checkExistingImages(
    loc,
    locGroup,
    endDate,
    startDate=None,
    dateID=None,
    maxCloudCover=None,
    setupImg=None,
    setupMap=None,
    setupActivityPrep=None,
    setupActivity=None,
    mode="initial_scan"
):
    PLANET_API_KEY = os.environ.get("PLANET_API_KEY")
    print(f"using API key: {PLANET_API_KEY}")
    session = requests.Session()
    session.auth = (PLANET_API_KEY, "")

    if mode == "initial_scan":
        if checkLocationFileStatus(loc, "00DownStatus") == "complete":
            print("All downloads marked complete for", loc, " -- skipping image check.")
            return "none", "none"
        locsRunningOrders = checkRunningOrders()
    else:
        saveConvexHull(loc)  # Already saving for update
        logger.debug(f"Checking existing images for {loc} in {locGroup} from {startDate} to {endDate}:")
        locsRunningOrders = checkRunningOrders(update_orders=True)

    logger.debug(f"locs with running Planet orders: {set(locsRunningOrders)}")

    if loc in locsRunningOrders:
        logger.debug(f"Downloads currently running for {loc} -- skipping image check.")
        time.sleep(180)
        return "none", "none"

    GEEbucket = checkLocationFileStatus(loc, "bucket")
    logger.debug(f"Looking up existing imagery for {loc}...")

    # Always initialize 'existing' early
    existing = []

    if mode == "update":
        pattern = r"{}/(.*?)_3B_AnalyticMS".format(loc)
    else:
        pattern = r"{}/(.*?)_3B_".format(loc)

    tif_pattern = r"\.tif$"

    # Decide whether to use Earth Engine or GCS based on GCS status
    stored_in_gcs = checkLocationFileStatus(loc, "stored_in_gcs")
    if stored_in_gcs == 0:
        bucketFolder = ee.data.listAssets(f"projects/{GEEbucket}/assets/PS_imgs")["assets"][0]["id"].split("/")[-1]
        os.system(
            f'earthengine ls "projects/{GEEbucket}/assets/PS_imgs/{bucketFolder}/{loc}"> ./temp/alreadyUp{loc}.txt'
        )
        if mode == "update":
            with open(f"./temp/alreadyUp{loc}.txt", "r") as file:
                for line in file:
                    match = re.search(pattern, line.strip())
                    if match:
                        existing.append(match.group(1))
    elif stored_in_gcs == 1:
        os.system(
            f"gcloud storage ls --recursive gs://ps-imgs-mai1/{loc} > ./temp/alreadyUp{loc}.txt"
        )
    else:
        if mode == "initial_scan":
            updateLocationFileStatus(loc, "stored_in_gcs", 1, replace=True)
            os.system(
                f"gcloud storage ls --recursive gs://ps-imgs-mai1/{loc} > ./temp/alreadyUp{loc}.txt"
            )

    # Parse existing images
    with open(f"./temp/alreadyUp{loc}.txt", "r") as file:
        for line in file:
            line = line.strip()
            if re.search(pattern, line):
                tif_match = re.search(tif_pattern, line)
                if tif_match:
                    existing.append(line)

    # Log and update totals depending on GCS flag
    if stored_in_gcs == 0:
        logger.debug(f"Found {len(existing)} images for {loc} -- updating location file.")
        updateLocationFileStatus(loc, "totalDownloaded", len(existing), replace=True)
    elif stored_in_gcs == 1:
        logger.debug(f"Found {len(existing) / 2} images for {loc} -- updating location file.")
        updateLocationFileStatus(loc, "totalDownloaded", len(existing) / 2, replace=True)

    # Always create convex hull if needed
    saveConvexHull(loc)

    # Then load convex hull
    with open(f"./temp/Jsons/{loc}feature.geojson") as f:
        geojson_data = json.loads(f.read())  # Load the convex hull json

    # Look for an anchor image
    search_percent = 99
    while True:
        search_para_1 = fn_search_para_1()
        search_para_1["filter"]["config"][0]["config"]["coordinates"] = geojson_data["geometry"]["coordinates"]
        search_para_1["filter"]["config"][1]["config"]["gte"] = "2020-01-01T00:00:00Z"
        search_para_1["filter"]["config"][1]["config"]["lte"] = endDate + "T23:59:59Z"
        search_para_1["filter"]["config"][2]["config"]["lte"] = 0
        search_para_1["filter"]["config"][3]["config"]["lte"] = 0
        search_para_1["filter"]["config"][4]["config"]["gte"] = search_percent
        search_para_1["filter"]["config"][5]["config"]["gte"] = search_percent
        search_para_1["filter"]["config"][6]["config"] = ["true"]

        search_percent -= 1

        # Search for anchor products
        max_retries = 10
        for attempt in range(max_retries + 1):
            try:
                search_response = session.post(search_url, json=search_para_1)
                if search_response.status_code == 200:
                    break
                else:
                    logger.debug(f"Request attempt {attempt + 1} failed with status code: {search_response.status_code}")
            except Exception as e:
                logger.debug(f"Request attempt {attempt + 1} failed with error: {e}")
            if attempt < max_retries:
                time.sleep(retry_interval)
            else:
                logger.debug("Maximum retry attempts reached. Request failed.")

        geojson = search_response.json()
        features = geojson["features"]

        # Loop over pages
        while True:
            next_link = geojson.get("_links", {}).get("_next")
            if next_link is None:
                break

            page_url = next_link
            for attempt in range(max_retries + 1):
                try:
                    r = session.get(page_url)
                    if r.status_code == 200:
                        break
                    else:
                        logger.debug(f"Request attempt {attempt + 1} failed with status code: {r.status_code}")
                except Exception as e:
                    logger.debug(f"Request attempt {attempt + 1} failed with error: {e}")
                if attempt < max_retries:
                    time.sleep(retry_interval)
                else:
                    logger.debug("Maximum retry attempts reached. Request failed.")
            geojson = r.json()
            features += geojson["features"]

        if len(features) != 0:
            logger.debug(f"Image found for anchoring with search_percent = {search_percent}")
            break

    # Retrieve product IDs
    product_ids = []
    for i in features:
        product_ids.append(i["id"])

    # Arbitrarily pick last image for anchoring
    forAnchoring = product_ids[-1]

    # Search for available images
    if mode == "update":
        features_sr = searchAvailableImages(geojson_data, endDate, maxCloudCover, startDate)
    else:
        features_sr = searchAvailableImages(geojson_data, endDate, maxCloudCover)

    # Get new products
    product_ids = []
    for i in features_sr:
        product_ids.append(i["id"])
    new_products = remove_overlapping_strings(product_ids, existing)

    logger.debug(f"{loc} total number of products available: {len(product_ids)}")
    logger.debug(f"{loc} total number of existing products: {len(existing)}")
    logger.debug(f"{loc} number of new products available: {len(new_products)}")

    # Completion check
    threshold = 6 if mode == "update" else 10

    if len(new_products) < threshold:
        if mode == "initial_scan":
            print(f"All images already downloaded for {loc} -- marking complete")

        if stored_in_gcs == 1:
            deleteDuplicates_gcs(loc)
            _, image_IDs = extract_harmonized_files_and_ids(f"gs://ps-imgs-mai1/{loc}", loc)
            logger.debug("image_IDs: %d, %s", len(image_IDs), image_IDs[0:5])

            features_json, features = process_json_files(loc)
            geojson = create_geojson(features_json)

            client = storage.Client()
            bucket = client.bucket("ps-imgs-mai1")
            blob = bucket.blob(f"imgProperties/{loc}.geojson")
            blob.upload_from_string(json.dumps(geojson), content_type="application/json")
            logger.debug(f"Image properties uploaded for {loc}")

        elif stored_in_gcs == 0:
            deleteDuplicates(loc)

        updateLocationFileStatus(loc, "00DownStatus", "complete", replace=True)

        assignForProcessing(
            loc,
            setupImg='Apr24' if mode == "update" else setupImg,
            setupMap='MpM6' if mode == "update" else setupMap,
            setupActivityPrep='June06maxpMax' if mode == "update" else setupActivityPrep,
            setupActivity='exportAct5' if mode == "update" else setupActivity,
        )

        if mode == "update":
            updateLocationFileStatus(loc, "lastImageUpdate", endDate, replace=True)
            updateUpdateProcess(loc, "imageDownload", "", dateID, "Status", "complete", replace=True)

        print(f"Downloading complete for {loc}")

    return new_products, forAnchoring


def searchAvailableImages(geojson_data, endDate, maxCloudCover, startDate=None):
    PLANET_API_KEY = os.environ.get("PLANET_API_KEY")
    session = requests.Session()
    session.auth = (PLANET_API_KEY, "")

    logger.debug(f"Requesting available images for cloud cover {maxCloudCover}...")

    # Create new search parameters to capture all images of interest
    search_para_2 = fn_search_para_2()
    search_para_2["filter"]["config"][0]["config"]["coordinates"] = geojson_data["geometry"]["coordinates"]
    search_para_2["filter"]["config"][1]["config"]["gte"] = startDate if startDate else "2016-01-01T00:00:00Z"
    search_para_2["filter"]["config"][1]["config"]["lte"] = endDate + "T23:59:59Z"
    search_para_2["filter"]["config"][2]["config"]["lte"] = maxCloudCover
    print(search_para_2)

    # Search for products using the Data API
    for attempt in range(max_retries + 1):
        try:
            search_response = session.post(search_url, json=search_para_2)
            if search_response.status_code == 200:
                break
            else:
                logger.debug(f"Request attempt {attempt + 1} failed with status code: {search_response.status_code}")
        except Exception as e:
            logger.debug(f"Request attempt {attempt + 1} failed with error: {e}")
        if attempt < max_retries:
            time.sleep(retry_interval)
        else:
            logger.debug("Maximum retry attempts reached. Request failed.")

    geojson = search_response.json()
    features = geojson["features"]

    # Paginate through all available results
    while True:
        next_link = geojson.get("_links", {}).get("_next")
        if not next_link:
            break

        page_url = next_link
        for attempt in range(max_retries + 1):
            try:
                r = session.get(page_url)
                if r.status_code == 200:
                    break
                else:
                    logger.debug(f"Request attempt {attempt + 1} failed with status code: {r.status_code}")
            except Exception as e:
                logger.debug(f"Request attempt {attempt + 1} failed with error: {e}")
            if attempt < max_retries:
                time.sleep(retry_interval)
            else:
                logger.debug("Maximum retry attempts reached. Request failed.")

        geojson = r.json()
        features += geojson["features"]

    # Filter for SR images with valid UDM2
    features_sr = [
        feature for feature in features
        if "ortho_analytic_4b_sr" in feature["assets"]
        and "ortho_udm2" in feature["assets"]
    ]

    return features_sr
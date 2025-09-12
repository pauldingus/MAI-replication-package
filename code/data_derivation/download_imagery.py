import os, re, json, time, requests, subprocess, geojson, logging, warnings
from datetime import datetime
from shapely.geometry import shape
import pandas as pd
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor, as_completed
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
session = requests.Session()
colspecs = [(0, 24), (26, 38), (40, 82), (84, 93), (95, 1000)]
max_retries = 10
retry_interval = 5
endDate = datetime.today().strftime("%Y-%m-%d")
date_pattern1 = r"_20\d{2}-\d{2}-\d{2}_"
date_pattern2 = r"PSScene/20\d{2}\d{2}\d{2}_"
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

# Custom JSON encoder for Timestamps
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        return super().default(obj)


# -------------------------------------------------------------------------------------------------------------------------------
# DOWNLOADER
# -------------------------------------------------------------------------------------------------------------------------------


def downloader(
    loc,
    locGroup,
    endDate="2024-12-31",
    maxRunningDownloads=10,
    maxCloudCover=50,
    planet_api_key=None,
    gcs_bucket=None,
    private_key="",
):
    
    # Check if this is no-API-key mode (most basic preview)
    if planet_api_key is None or planet_api_key == "":
        print(f"\n=== REQUEST STRUCTURE OVERVIEW for {loc} ===")
        print("No Planet API key provided. Showing request structure overview...")
        
        show_request_structure_overview(
            loc,
            locGroup,
            endDate,
            maxCloudCover,
            gcs_bucket
        )
        
        print(f"\nTo get image counts and detailed preview, provide a Planet API key.")
        print(f"To actually download, provide both Planet API key and GCS bucket.")
        print("="*50)
        return "structure_overview_completed"
    
    # Check if this is preview mode (no bucket provided but has API key)
    preview_mode = gcs_bucket is None or gcs_bucket == ""
    
    if preview_mode:
        print(f"\n=== PREVIEW MODE for {loc} ===")
        print("No GCS bucket provided. Showing download preview...")
        
        # Run preview analysis
        preview_info = get_download_preview(
            loc,
            locGroup,
            endDate,
            maxCloudCover,
            planet_api_key
        )
        
        print(f"\n--- Download Preview for {loc} ---")
        print(f"Location: {loc}")
        print(f"Location Group: {locGroup}")
        print(f"End Date: {endDate}")
        print(f"Max Cloud Cover: {maxCloudCover}%")
        print(f"Number of images to download: {preview_info['image_count']}")
        print(f"Estimated total file size: {preview_info.get('estimated_size', 'Unknown')}")
        
        if preview_info['image_count'] > 0:
            print(f"\nPlanet Order Parameters:")
            print(f"  - Product Bundle: analytic_sr_udm2")
            print(f"  - Item Type: PSScene")
            print(f"  - Tools: Clip, COG format, Harmonize to Sentinel-2")
            print(f"  - Number of chunks: {(preview_info['image_count'] + 498) // 499}")
            print(f"  - Items per chunk: {min(499, preview_info['image_count'])}")
            
            if preview_info.get('sample_product_ids'):
                print(f"\nSample Product IDs (first 5):")
                for i, pid in enumerate(preview_info['sample_product_ids'][:5], 1):
                    print(f"  {i}. {pid}")
        else:
            print("No new images found to download.")
            
        print(f"\nTo actually download, provide a GCS bucket name.")
        return "preview_completed"
    
    # Extract json of each feature, convert to convex hull geometry, and export to temp folder as a json
    saveConvexHull(loc, locGroup)

    # While loc downloads not completed
    complete = False
    status = ""
    downloads_initiated = 0
    while not complete:
        # find locs whose imagery downloads are processing and check if they have finished:
        if status == "initiated":
            status = checkExistingImages(
                loc,
                locGroup,
                endDate,
                maxCloudCover,
                planet_api_key,
                status,
                gcs_bucket
            )

            # Check if downloads are complete
            if status == "complete":
                complete = True

            # check if the location has reached the attempted downloads limit -- if so, mark as failed
            if downloads_initiated > 4:
                print(f"{loc} reached download limit without success -- marking as failed.")
                complete = True
                response_text = "Failed due to repeated unsuccessful attempts."
                return response_text

        # If no orders are currently running for this location
        running_locs = checkRunningOrders(planet_api_key)
        if loc not in running_locs and status != "complete":
            status = requestDownloads(
                loc,
                locGroup,
                endDate,
                private_key,
                maxRunningDownloads,
                maxCloudCover,
                planet_api_key,
                gcs_bucket
            )
            downloads_initiated += 1

        time.sleep(10)

    print(f"All downloads complete for {loc}!")


def requestDownloads(
    loc,
    locGroup,
    endDate,
    private_key,
    maxRunningDownloads,
    maxCloudCover,
    planet_api_key=None,
    gcs_bucket=None
):
    # Function to request the downloads needed for a given location.
    # Inputs:
    # new_products [list]:      list of sr-corrected image product ids that should be downloaded from Planet
    # forAnchoring [string]:    planet image product id that should be used for anchoring the downloaded images
    # private_key [string]:     file path to planet encrypted private key

    # Validation: Cannot request downloads without a bucket
    if gcs_bucket is None or gcs_bucket == "":
        logger.error(f"Cannot request downloads for {loc}: No GCS bucket provided")
        return "failed"
    
    # Validation: Cannot request downloads without an API key
    if planet_api_key is None or planet_api_key == "":
        logger.error(f"Cannot request downloads for {loc}: No Planet API key provided")
        return "failed"

    session = requests.Session()
    session.auth = (planet_api_key, "")

    new_products, forAnchoring, status = checkExistingImages(
        loc,
        locGroup,
        endDate,
        maxCloudCover,
        planet_api_key,
        "initiated",  # Default status when requesting downloads
        gcs_bucket
    )

    if status == "complete":
        return status
    
    logger.debug(f"New SR products found: {new_products[0:5]} ...")
    orders = []

    with open(f"./temp/Jsons/{loc}feature.geojson") as f:
        geojson_data = json.loads(f.read())

    running = checkRunningOrders(planet_api_key)
    len_running = len(running)

    if len_running > maxRunningDownloads:
        print(f"{len_running} running orders -- waiting for some to finish before starting {loc}...")
        time.sleep(120)

    else:
        max_retries = 10

        if len(new_products) >= 10:
            print(f"SR download initiated for {loc} -- requesting {len(new_products)} products")
            # Database update to mark location as initiated
            #updateLocationFileStatus(loc, "00DownStatus", "initiated", replace=True)
            status = "initiated"  # In lieu of the above line, use a status variable to track initiation

            for i in range(0, len(new_products), 499):
                logger.debug(f"Chunk {i}")
                itemIDs = []
                itemIDs.extend(new_products[i : i + 499])
                itemIDs.append(str(forAnchoring))

                order_payload = fn_order_payload()
                order_payload["products"][0]["item_ids"] = itemIDs
                order_payload["name"] = f"{loc} chunk {i}"
                order_payload["delivery"]["google_cloud_storage"]["bucket"] = gcs_bucket
                order_payload["delivery"]["google_cloud_storage"]["path_prefix"] = loc
                order_payload["delivery"]["google_cloud_storage"]["credentials"] = (private_key)
                order_payload["tools"][0]["clip"]["aoi"] = geojson_data["geometry"]

                for attempt in range(max_retries + 1):
                    order_response = session.post(order_url, json=order_payload)
                    try:
                        if order_response.status_code == 202:
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
                        status = "failed"
                        return status
                
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
                        logger.debug(f"SR download marked complete for {loc} since no new items were registered.")
                        status = "complete"
                        return status

                time.sleep(60)
    
    # Return status of initiated if we reach this point (normal case)
    return "initiated"


def checkExistingImages(
    loc,
    locGroup,
    endDate,
    maxCloudCover,
    planet_api_key=None,
    status="",
    gcs_bucket=None
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

    # Validation: Cannot check existing images without an API key
    if planet_api_key is None or planet_api_key == "":
        logger.error(f"Cannot check existing images for {loc}: No Planet API key provided")
        return [], "", "failed"
    
    # Validation: Cannot check existing images without a bucket (in normal mode)
    if gcs_bucket is None or gcs_bucket == "":
        logger.error(f"Cannot check existing images for {loc}: No GCS bucket provided")
        return [], "", "failed"

    session = requests.Session()
    session.auth = (planet_api_key, "")

    logger.debug(f"Checking existing images for {loc} in {locGroup} up to {endDate}:")

    # Logic to skip if downloads already marked complete
    if status == "complete":
        print("All downloads marked complete for", loc, " -- skipping image check.")
        new_products, forAnchoring = ("none", "none")
        status = "complete"
        return new_products, forAnchoring, status

    # If image downloads already running, don't run.
    running = checkRunningOrders(planet_api_key)
    logger.debug(f"locs with running Planet orders: {set(running)}")
    if loc in running:
        logger.debug(f"Downloads currently running for {loc} -- skipping image check.")
        new_products, forAnchoring = ("none", "none")
        status = "initiated"
        time.sleep(180)

    else:
        # Search for collections that already exist for the location, and store them in a list
        logger.debug(f"Looking up existing imagery for {loc}...")
        pattern = r"{}/(.*?)_3B_".format(loc)  # pattern to search for
        tif_pattern = r"\.tif$"  # Matches strings ending with .tif

        # Retrieve list of existing images in GCS bucket
        os.system(f"gcloud storage ls --recursive gs://{gcs_bucket}/{loc} > ./temp/alreadyUp{loc}.txt")
        existing = []
        with open(f"./temp/alreadyUp{loc}.txt", "r") as file:
            for line in file:
                line = line.strip()
                if re.search(pattern, line):
                    tif_match = re.search(tif_pattern, line)
                    if tif_match:
                        existing.append(line)  # Append the entire

        logger.debug(f"Found {len(existing) / 2} images for {loc} -- updating location file.")
        # Logic to count unique images based on naming pattern
        #updateLocationFileStatus(loc, "totalDownloaded", len(existing) / 2, replace=True)

        # Open up the convex hull json
        with open(f"./temp/Jsons/{loc}feature.geojson") as f:
            geojson_data = json.loads(f.read())  

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
        features_sr = searchAvailableImgs(geojson_data, endDate, maxCloudCover, planet_api_key)

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
            deleteDuplicates_gcs(loc, gcs_bucket)
            # Extract harmonized files and image IDs
            _, image_IDs = extract_harmonized_files_and_ids(
                f"gs://{gcs_bucket}/{loc}", loc
            )
            logger.debug("image_IDs: %d, %s", len(image_IDs), image_IDs[0:5])

            # Process JSON files and create FeatureCollection
            features_json, features = process_json_files(loc, gcs_bucket)
            geojson = create_geojson(features_json)
            client = storage.Client()
            bucket = client.bucket(gcs_bucket)
            blob = bucket.blob(f"imgProperties/{loc}.geojson")
            blob.upload_from_string(
                json.dumps(geojson), content_type="application/json"
            )
            print("image properties uploaded", loc)

            # Database update to mark location complete
            #updateLocationFileStatus(loc, "00DownStatus", "complete", replace=True)
            status = "complete" # In lieu of the above line, use a status variable to track completion
            print(f"Downloading complete for {loc}")

    return new_products, forAnchoring, status


def searchAvailableImgs(geojson_data, endDate, maxCloudCover, planet_api_key=None):
    if planet_api_key is None or planet_api_key == "":
        logger.error("Cannot search available images: No Planet API key provided")
        return []
    
    logger.debug(f"Requesting available images for cloud cover {maxCloudCover}...")

    session = requests.Session()
    session.auth = (planet_api_key, "")

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


# -------------------------------------------------------------------------------------------------------------------------------
# PREVIEW FUNCTIONS
# -------------------------------------------------------------------------------------------------------------------------------

def show_request_structure_overview(loc, locGroup, endDate, maxCloudCover, gcs_bucket):
    """
    Show the structure of requests that would be made without making actual API calls.
    This gives users an overview when no API key is provided.
    """
    print(f"\n--- Request Structure Overview for {loc} ---")
    print(f"Location: {loc}")
    print(f"Location Group: {locGroup}")  
    print(f"End Date: {endDate}")
    print(f"Max Cloud Cover: {maxCloudCover}%")
    print(f"GCS Bucket: {gcs_bucket if gcs_bucket else 'Not provided (preview mode)'}")
    
    print(f"\n--- Planet API Requests That Would Be Made ---")
    
    print(f"\n1. Image Search Request:")
    print(f"   URL: https://api.planet.com/data/v1/quick-search")
    print(f"   Method: POST")
    print(f"   Purpose: Find available satellite imagery")
    print(f"   Filters:")
    print(f"     - Item Type: PSScene")
    print(f"     - Date Range: 2016-01-01 to {endDate}")
    print(f"     - Cloud Cover: ≤ {maxCloudCover}%")
    print(f"     - Assets Required: ortho_analytic_4b_sr, ortho_udm2")
    print(f"     - Geographic Area: Clipped to location boundaries")
    
    print(f"\n2. Anchor Image Search Request:")
    print(f"   URL: https://api.planet.com/data/v1/quick-search")
    print(f"   Method: POST")
    print(f"   Purpose: Find high-quality reference image for harmonization")
    print(f"   Filters:")
    print(f"     - Item Type: PSScene")
    print(f"     - Date Range: 2020-01-01 to {endDate}")
    print(f"     - Cloud Cover: 0%")
    print(f"     - Anomalous Pixels: 0%")
    print(f"     - Clear Confidence: ≥ 99%")
    print(f"     - Ground Control: Required")
    
    print(f"\n3. Download Order Request(s):")
    print(f"   URL: https://api.planet.com/compute/ops/orders/v2")
    print(f"   Method: POST")
    print(f"   Purpose: Download processed imagery")
    print(f"   Configuration:")
    print(f"     - Product Bundle: analytic_sr_udm2")
    print(f"     - Processing Tools:")
    print(f"       • Clip to area of interest")
    print(f"       • Convert to Cloud Optimized GeoTIFF (COG)")
    print(f"       • Harmonize radiometry to Sentinel-2")
    print(f"     - Delivery Method: Google Cloud Storage")
    if gcs_bucket:
        print(f"     - Destination Bucket: {gcs_bucket}")
        print(f"     - Path Prefix: {loc}")
    else:
        print(f"     - Destination Bucket: [Would be specified]")
        print(f"     - Path Prefix: {loc}")
    print(f"     - Chunking: ~499 images per order (Planet limit)")
    
    print(f"\n4. Order Status Monitoring:")
    print(f"   URL: https://api.planet.com/compute/ops/orders/v2?state=running&state=queued")
    print(f"   Method: GET")
    print(f"   Purpose: Monitor download progress")
    print(f"   Polling: Every 10 seconds until completion")
    
    if gcs_bucket:
        print(f"\n--- Google Cloud Storage Operations ---")
        print(f"   - List existing files: gs://{gcs_bucket}/{loc}")
        print(f"   - Upload image metadata: gs://{gcs_bucket}/imgProperties/{loc}.geojson")
        print(f"   - Remove duplicate files if found")
    else:
        print(f"\n--- Google Cloud Storage Operations (if bucket provided) ---")
        print(f"   - List existing files in bucket")
        print(f"   - Upload image metadata")
        print(f"   - Remove duplicate files if found")
    
    print(f"\n--- Authentication Required ---")
    print(f"   - Planet API Key: For all Planet API requests")
    print(f"   - Google Cloud Credentials: For GCS operations")
    print(f"   - Private Key: For Planet download delivery")


def get_download_preview(loc, locGroup, endDate, maxCloudCover, planet_api_key):
    """
    Get preview information about what would be downloaded without actually downloading.
    Returns a dictionary with image count and other useful information.
    """
    try:
        # Extract json of each feature, convert to convex hull geometry, and export to temp folder as a json
        saveConvexHull(loc, locGroup)
        
        # Open up the convex hull json
        with open(f"./temp/Jsons/{loc}feature.geojson") as f:
            geojson_data = json.loads(f.read())
        
        # Get available images using the same logic as the actual download
        features_sr = searchAvailableImgs(geojson_data, endDate, maxCloudCover, planet_api_key)
        
        # Get product IDs
        product_ids = [feature["id"] for feature in features_sr]
        
        # For preview, we don't check existing images since no bucket is provided
        # So we show all available images
        
        # Calculate estimated file size (rough estimate: ~50MB per image)
        estimated_size_mb = len(product_ids) * 50
        if estimated_size_mb > 1024:
            estimated_size = f"{estimated_size_mb / 1024:.1f} GB"
        else:
            estimated_size = f"{estimated_size_mb} MB"
        
        return {
            'image_count': len(product_ids),
            'sample_product_ids': product_ids[:10],  # First 10 for sample
            'estimated_size': estimated_size,
            'total_available': len(product_ids)
        }
        
    except Exception as e:
        logger.error(f"Error getting download preview for {loc}: {e}")
        return {
            'image_count': 0,
            'sample_product_ids': [],
            'estimated_size': 'Unknown',
            'error': str(e)
        }


# -------------------------------------------------------------------------------------------------------------------------------
# UTILITY FUNCTIONS
# -------------------------------------------------------------------------------------------------------------------------------

def checkRunningOrders(planet_api_key=None):
    if planet_api_key is None or planet_api_key == "":
        logger.warning("Cannot check running orders: No Planet API key provided")
        return []

    url = "https://api.planet.com/compute/ops/orders/v2?state=running&state=queued"
    headers = {
        "Authorization": f"api-key {planet_api_key}",
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


def remove_overlapping_strings(list1, list2):
    # checks for each of the strings in list1 whether it forms at least part of any of the strings in list2. We use it to filter out already downloaded imagery from the new set of tasks

    return [string for string in list1 if string not in "|".join(list2)]


def saveConvexHull(loc, locGroup):
    storage_client = storage.Client()
    bucketName = "mai_2023"
    bucket = storage_client.get_bucket(bucketName)
    country = locGroup.split("_")[1]
    countryCode = locGroup.split("_")[0]

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
    # Database updates to mark location as failed due to missing GeoJSON
    #updateLocationFileStatus(loc, "00DownStatus", "failed", replace=True)
    #updateLocationFileStatus(loc, "00aDownNoSRStatus", "failed", replace=True)
    #updateLocationFileStatus(loc, "notes", "missingGeoJSON", replace=True)
    # In lieu of database updates, we raise an exception to indicate failure
    raise Exception(
        f"GeoJSON not found for {loc}. Check that it is uploaded to google storage correctly"
    )


def deleteDuplicates_gcs(loc, gcs_bucket=None):
    """
    List all file names in a specific GCS folder (excluding paths), including subfolders.
    If a file name is already in the list, delete the duplicate file from GCS.

    Parameters:
    loc (str): The location identifier.
    gcs_bucket (str): The name of the GCS bucket containing the objects.
    """
    if gcs_bucket is None or gcs_bucket == "":
        logger.warning(f"Cannot delete duplicates for {loc}: No GCS bucket provided")
        return
    
    try:
        # Initialize the GCS client
        client = storage.Client()
        bucket = client.bucket(gcs_bucket)

        # Set to track unique file names
        unique_file_names = set()

        # List blobs within the specified folder
        blobs = client.list_blobs(gcs_bucket, prefix=loc)

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
            f"Processed {len(unique_file_names)} unique files in folder {loc} of bucket {gcs_bucket}"
        )

    except Exception as e:
        logger.error(f"Error processing folder {loc} in bucket {gcs_bucket}: {e}")


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
    # Extract bucket name from GCS_BUCKET path (e.g., "gs://bucket-name/location" -> "bucket-name")
    bucket_name = GCS_BUCKET.split("/")[2]
    harmonized_files = [
        file.replace(f"gs://{bucket_name}/{loc}/", "").replace(
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

        return json_data # Or return whatever you need
    except Exception as e:
        print(f"Error processing blob {blob_name}: {e}")
        return None, None


def process_json_files(loc, gcs_bucket=None, MAX_WORKERS=10):
    if gcs_bucket is None or gcs_bucket == "":
        logger.warning(f"Cannot process JSON files for {loc}: No GCS bucket provided")
        return [], []
    
    _, image_IDs = extract_harmonized_files_and_ids(f"gs://{gcs_bucket}/{loc}", loc)
    # print('image_IDs',image_IDs)
    client = storage.Client()
    imageBucket = client.bucket(gcs_bucket)
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
                {"type": "AssetFilter", "config": ["ortho_udm2"]},
            ],
        },
    }
    return search_para_2


# https://developers.planet.com/docs/apis/data/searches-filtering/#stringinfilter
# Order API parameters
def fn_order_payload():
    order_payload = {
        "name": None,
        "order_type": "partial",
        "products": [
            {
                "item_ids": None,  # to be filled in later
                "item_type": "PSScene",
                "product_bundle": "analytic_sr_udm2",  # https://developers.planet.com/apis/orders/product-bundles-reference/
            }
        ],
        "tools": [
            {
                "clip": {
                    "aoi": None  #
                }
            },
            {"file_format": {"format": "COG"}},
            {"harmonize": {"target_sensor": "Sentinel-2"}},
        ],
        "delivery": {
            "google_cloud_storage": {
                "bucket": None,
                "path_prefix": None,
                "credentials": None,
            }
        },
    }
    return order_payload

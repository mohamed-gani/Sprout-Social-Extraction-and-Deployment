import requests
import json
import csv
from google.cloud import storage
from google.cloud import secretmanager
from datetime import datetime
import logging
from google.oauth2 import service_account

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_secret(secret_name, project_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode('UTF-8')


def fetch_and_store_data(access_token, customer_id, storage_client, BUCKET_NAME, JSON_FILE_NAME, CSV_FILE_NAME):
    # Define the API endpoint
    url = f'https://api.sproutsocial.com/v1/{customer_id}/analytics/profiles'

    # Define the payload data for the POST request
    payload_data = {
    "filters": [
      "customer_profile_id.eq(2185669, 3276812, 4007857)",
      "reporting_period.in(2023-01-01...2023-08-31)"
    ],
    "metrics": ["lifetime_snapshot.followers_count","lifetttime.net_follower_growth","Likes","impressions","lifetime_snapshot.fans_count","net_fan_growth","fans_gained","fans_lost","lifetttimeposts_sent_count","facebook.impressions","lifetttime.engagements_v2","reactions","likes","lifetttime.comments_count","shares_count","post_link_clicks","profile_actions","post_content_clicks_other","post_content_clicks","post_photo_view_clicks","post_video_play_clicks","video_views"],
    "page": 1
    }

    # Define headers with your access token and content type
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    # Send the POST request with the payload data
    response = requests.post(url, json=payload_data, headers=headers)

    # Check the response status code
    if response.status_code == 200:
        response_data = response.json()

        # Save the response to Google Cloud Storage
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(JSON_FILE_NAME)  # Name of the file in GCS
        blob.upload_from_string(
            json.dumps(response_data, indent=4),
            content_type='application/json'
        )
        convert_csv(response_data, storage_client, BUCKET_NAME, CSV_FILE_NAME)
        logger.info(f"POST request was successful. Response saved to GCS: {BUCKET_NAME}/{JSON_FILE_NAME}")
    else:
        # Request failed, return the error message
        logger.error(f"Error: {response.status_code} - {response.text}")

def convert_csv(response_data, storage_client, BUCKET_NAME, CSV_FILE_NAME):

    # Define social platforms
    social_platforms = {
        2185669: "Facebook",
        4007857: "Instagram",
        3276812: "LinkedIn"
    }

    # Create a list to store the CSV rows
    csv_rows = []

    # Process each entry in the JSON data
    for entry in response_data["data"]:
        customer_id = entry["dimensions"]["customer_profile_id"]
        social_platform = social_platforms.get(customer_id, "Unknown")
        
        # Add the social platform information
        entry["dimensions"]["Social_Platform"] = social_platform
        
        # Extract relevant metrics
        metrics = entry["metrics"]
        
        # Define a default value (0) for missing metrics
        default_metric = 0
        
        row = [
            customer_id,
            social_platform,
            metrics.get("comments_count", default_metric),
            metrics.get("engagements_v2", default_metric),
            metrics.get("fans_gained", default_metric),
            metrics.get("fans_lost", default_metric),
            metrics.get("impressions", default_metric),
            metrics.get("lifetime_snapshot.fans_count", default_metric),
            metrics.get("lifetime_snapshot.followers_count", default_metric),
            metrics.get("likes", default_metric),
            metrics.get("net_fan_growth", default_metric),
            metrics.get("net_follower_growth", default_metric),
            metrics.get("post_content_clicks", default_metric),
            metrics.get("post_content_clicks_other", default_metric),
            metrics.get("post_link_clicks", default_metric),
            metrics.get("post_photo_view_clicks", default_metric),
            metrics.get("post_video_play_clicks", default_metric),
            metrics.get("posts_sent_count", default_metric),
            metrics.get("profile_actions", default_metric),
            metrics.get("reactions", default_metric),
            metrics.get("shares_count", default_metric),
            metrics.get("video_views", default_metric)
        ]
        
        # Append row to the list
        csv_rows.append(row)

    # Write data to CSV file in GCS
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(CSV_FILE_NAME)

    with blob.open('w') as file:
        writer = csv.writer(file)
        
        # Write header
        header = [
            "Customer_ID",
            "Social_Platform",
            "Comments_Count",
            "Engagements_v2",
            "Fans_Gained",
            "Fans_Lost",
            "Impressions",
            "Lifetime_Snapshot_Fans_Count",
            "Lifetime_Snapshot_Followers_Count",
            "Likes",
            "Net_Fan_Growth",
            "Net_Follower_Growth",
            "Post_Content_Clicks",
            "Post_Content_Clicks_Other",
            "Post_Link_Clicks",
            "Post_Photo_View_Clicks",
            "Post_Video_Play_Clicks",
            "Posts_Sent_Count",
            "Profile_Actions",
            "Reactions",
            "Shares_Count",
            "Video_Views"
        ]
        writer.writerow(header)
        
        # Write rows
        writer.writerows(csv_rows)

    logger.info(f'CSV file "{CSV_FILE_NAME}" created successfully in GCS bucket "{BUCKET_NAME}".')

def main(request):
    project_id = "exportsproutinformation"
    service_account_json = get_secret("google-cloud-credentials", project_id)
    access_token = get_secret("sprout-access-token", project_id)
    customer_id = get_secret("sprout-customer-id", project_id)
    credentials = service_account.from_service_account_info(json.loads(service_account_json))
    storage_client = storage.Client(credentials=credentials)

    BUCKET_NAME = 'sprout_responses_json'
    current_datetime = datetime.now().strftime("%Y-%m-%d")
    JSON_FILE_NAME = f'sproutsocial_post_response_{current_datetime}.json'
    CSV_FILE_NAME = f'customer_data_{current_datetime}.csv'

    fetch_and_store_data(access_token, customer_id, storage_client, BUCKET_NAME, JSON_FILE_NAME, CSV_FILE_NAME)

    return f"Data processed and saved to GCS: {BUCKET_NAME}/{JSON_FILE_NAME} and {BUCKET_NAME}/{CSV_FILE_NAME}"
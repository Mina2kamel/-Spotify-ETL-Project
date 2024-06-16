import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')

    # Initialize Spotify client
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    # Fetch playlist tracks
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=1333723a6eff4b7f&nd=1&dlsi=3998e1ebc6224c5e"
    playlist_URI = playlist_link.split('/')[-1].split('?')[0]
    spotify_data = sp.playlist_tracks(playlist_URI)

    # Upload data to S3
    client = boto3.client('s3')
    file_name = f"spotify_raw_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
    s3_key = f"raw_data/to_processed/{file_name}"

    client.put_object(
        Bucket="spotify-etl-mina",
        Key=s3_key,
        Body=json.dumps(spotify_data)
    )

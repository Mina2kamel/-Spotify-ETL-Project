import json
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime

def extract_album_data(row):
    album = row['track']['album']
    return {
        'album_id': album['id'],
        'name': album['name'],
        'release_date': album['release_date'],
        'total_tracks': album['total_tracks'],
        'url': album['external_urls']['spotify']
    }

def extract_song_data(row):
    track = row['track']
    album = track['album']
    return {
        'song_id': track['id'],
        'song_name': track['name'],
        'duration_ms': track['duration_ms'],
        'url': track['external_urls']['spotify'],
        'popularity': track['popularity'],
        'song_added': row['added_at'],
        'album_id': album['id'],
        'artist_id': album['artists'][0]['id']
    }

def extract_artist_data(row):
    return [
        {
            'artist_id': artist['id'],
            'artist_name': artist['name'],
            'external_url': artist['external_urls']['spotify']
        }
        for artist in row['track']['artists']
    ]

def create_dataframe(data, extract_func, drop_column):
    data_list = [extract_func(row) for row in data['items']]
    df = pd.DataFrame(data_list)
    df = df.drop_duplicates(subset=[drop_column])
    return df

def album_data(data):
    df = create_dataframe(data, extract_album_data, 'album_id')
    df['release_date'] = pd.to_datetime(df['release_date'])
    return df

def songs_data(data):
    df = create_dataframe(data, extract_song_data, 'song_id')
    df['song_added'] = pd.to_datetime(df['song_added'])
    return df

def artists_data(data):
    artist_list = [artist for row in data['items'] for artist in extract_artist_data(row)]
    df = pd.DataFrame(artist_list)
    df = df.drop_duplicates(subset=['artist_id'])
    return df

def save_to_s3(client, bucket, key, dataframe):
    buffer = StringIO()
    dataframe.to_csv(buffer, index=False)
    content = buffer.getvalue()
    client.put_object(Bucket=bucket, Key=key, Body=content)

def lambda_handler(event, context):
    client = boto3.client('s3')
    bucket = "spotify-etl-mina"
    prefix = "raw_data/to_processed/"

    spotify_data = []
    spotify_keys = []

    for file in client.list_objects(Bucket=bucket, Prefix=prefix)['Contents']:
        if file['Key'].endswith(".json"):
            response = client.get_object(Bucket=bucket, Key=file['Key'])
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file['Key'])


    for data in spotify_data:
        album_df = album_data(data)
        artists_df = artists_data(data)
        songs_df = songs_data(data)

        now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        save_to_s3(client, bucket, f"transformed_data/album_data/album_transformed_{now}.csv", album_df)
        save_to_s3(client, bucket, f"transformed_data/artist_data/artist_transformed_{now}.csv", artists_df)
        save_to_s3(client, bucket, f"transformed_data/song_data/song_transformed_{now}.csv", songs_df)


    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, bucket, 'raw_data/processed/' + key.split("/")[-1])
        s3_resource.Object(bucket, key).delete()

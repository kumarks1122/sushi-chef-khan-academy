from __future__ import print_function
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import tempfile
import pdb
import json
import csv
import os
import youtube_dl
import shutil
import requests

class DownloadTn():
  """docstring for DownloadTn
    reading and downloading KA content data youtube
  """

  def __init__(self):
    self.contents = []
    self.youtube = None
    self.MP4 = "mp4"
    self.videos_path = "./tn_videos"
    self.thumbnails_path = "./tn_images"
    self.uploaded_contents = []

  def process_content(self, video_data):
    video_id = video_data["id"]["videoId"]
    content = {}
    content['title'] = video_data['snippet']['title']
    content['description'] = video_data['snippet']['description']
    content['youtube_id'] = video_id
    content['published_at'] = video_data['snippet']['publishedAt']
    download_settings = {}
    destination_path = os.path.join(tempfile.gettempdir(), "{}.mp4".format(video_id))
    download_settings["outtmpl"] = destination_path
    download_settings["format"] = "[ext=mp4]"
    thumbnail_url = video_data["snippet"]["thumbnails"]["high"]["url"]
    with youtube_dl.YoutubeDL(download_settings) as ydl:
      ydl.download(["https://www.youtube.com/watch?v={}".format(video_id)])
      video_file_path = "{}/{}.{}".format(self.videos_path, video_id, self.MP4)
      thumbnail_file_path = "{}/{}.jpg".format(self.thumbnails_path, video_id)

      with open(destination_path, "rb") as dlf, open(video_file_path, 'wb') as destf:
        shutil.copyfileobj(dlf, destf)
        f = open(thumbnail_file_path,'wb')
        f.write(requests.get(thumbnail_url).content)
        f.close()

        content["video_name"] = "{}.mp4".format(video_id)
        drive_file = self.service.files().create(body={'name': content["video_name"], "parents": ["17VgIHddGW24Yd0hWIoALnDt0srOc1KZ9"]}, media_body=video_file_path).execute()
        content['drive_file_path'] = "https://drive.google.com/open?id=" + drive_file['id']

        # if generation[content["kind"]] < 31:
        # pdb.set_trace()
        content["thumbnail_file_name"] = "{}.jpg".format(video_id)

        drive_file = self.service.files().create(body={'name': content["thumbnail_file_name"], "parents": ["1bh6IszPGB2_c-TEVhoOwdRfYpTVG4Hnh"]}, media_body=thumbnail_file_path).execute()
        content['thumbnail_file_path'] = "https://drive.google.com/open?id=" + drive_file['id']

    self.contents.append(content)
    with open('TAMILNADU_contents.json', 'w') as outfile:
      json.dump(self.contents, outfile)

  def list_all_contents(self):
    # with open('TAMILNADU_contents.json', 'r') as outfile:
    #   self.contents = json.load(outfile)

    for content in self.contents:
      self.uploaded_contents.append(content['youtube_id'])


    page_token = ""
    while True:
      response = self.youtube.search().list(
        type='video',
        part='id,snippet',
        maxResults=50,
        pageToken=page_token,
        channelId="UC5qNUOOTmdBXS5A4Bj6UuRg"
      ).execute()

      for search_result in response.get('items', []):
        if search_result["id"]["videoId"] not in self.uploaded_contents:
          self.process_content(search_result)
          
        # Process change
      page_token = response.get('nextPageToken', "")
      # pdb.set_trace()
      if page_token is "":
        break

  def main(self):
    SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/youtube"]
    store = file.Storage('credentials.json')
    creds = store.get()
    if not creds or creds.invalid:
      flow = client.flow_from_clientsecrets('client_secret.json', SCOPES)
      creds = tools.run_flow(flow, store)
    self.youtube = build('youtube', 'v3', http=creds.authorize(Http()))
    self.service = build('drive', 'v3', http=creds.authorize(Http()))
    self.list_all_contents()

if __name__ == '__main__':
  chef = DownloadTn()
  chef.main()
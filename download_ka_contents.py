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

class DownloadKa():
  """docstring for DownloadKa
    reading and downloading KA content data youtube
  """

  def __init__(self):
    self.contents = []
    self.youtube = None
    self.MP4 = "mp4"
    self.videos_path = "./ka_videos"
    self.thumbnails_path = "./ka_images"

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
        drive_file = self.service.files().create(body={'name': content["video_name"], "parents": ["1vvXZDznBqe6eba42DluS_C52lNXYZqwx"]}, media_body=video_file_path).execute()
        content['drive_file_path'] = "https://drive.google.com/open?id=" + drive_file['id']

        # if generation[content["kind"]] < 31:
        # pdb.set_trace()
        content["thumbnail_file_name"] = "{}.jpg".format(video_id)

        drive_file = self.service.files().create(body={'name': content["thumbnail_file_name"], "parents": ["1GTE3y8SrRxqj-I7IBIvpwFS1x8_AvwXo"]}, media_body=thumbnail_file_path).execute()
        content['thumbnail_file_path'] = "https://drive.google.com/open?id=" + drive_file['id']

    self.contents.append(content)
    with open('KARNATAKA_contents.json', 'w') as outfile:
      json.dump(self.contents, outfile)

  def list_all_contents(self):
    page_token = ""
    while True:
      response = self.youtube.search().list(
        type='video',
        part='id,snippet',
        maxResults=50,
        pageToken=page_token,
        channelId="UClHj-U2Ec9W7Jt2sHiwQwAw"
      ).execute()

      for search_result in response.get('items', []):
        self.process_content(search_result)
        # Process change
      page_token = response.get('nextPageToken', "")
      pdb.set_trace()
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
  chef = DownloadKa()
  chef.main()
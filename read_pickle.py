from __future__ import print_function
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import pickle
import ricecooker
import pdb
import json
import csv
import os

class ReadPickle():
  """docstring for ReadPickle
    reading and extracting content data from pickle file
  """

  def __init__(self, pickle_path):
    self.pickle_path = pickle_path
    self.contents = []
    self.filtered = []
    self.service = None
    self.uploaded_contents = None

  def extract_content(self, content, hierarchy_path):
    content_data = content.to_dict()
    content_data["hierarchy_path"] = hierarchy_path
    content_video_file = None
    try:
      for content_file in content.files:
        if isinstance(content_file, ricecooker.classes.files.VideoFile):
          content_video_file = content_file
          break

      if isinstance(content, ricecooker.classes.nodes.VideoNode) and content_data["content_id"] not in self.uploaded_contents and content_video_file != None and content_video_file.filename != None:
        khan_file_path = ricecooker.config.get_storage_path(content_video_file.filename)
        file_stat = os.stat(khan_file_path)
        if file_stat.st_size <= 50000000:
          print('added1'+content_data["content_id"])

          content_data.pop("questions", None)
          content_data.pop("tags", None)
          content_data.pop("extra_fields", None)
          content_data['drive_file_name'] = content_video_file.filename
          drive_file = self.service.files().create(body={'name': content_data['drive_file_name'], "parents": ["15YwqoKbJLMTwC7Hm03QZCr9dGVxmmk2K"]}, media_body=khan_file_path).execute()
          content_data['drive_file_path'] = "https://drive.google.com/open?id=" + drive_file['id']

          # if generation[content_data["kind"]] < 31:
          # pdb.set_trace()
          content_data["thumbnail_file_name"] = ""
          if content.thumbnail != None:
            content_data["thumbnail_file_name"] = content.thumbnail.filename
            drive_file = self.service.files().create(body={'name': content_data["thumbnail_file_name"], "parents": ["1K5byDiFyEHqtjSmv36O4_oZjhh3tskAJ"]}, media_body=ricecooker.config.get_storage_path(content.thumbnail.filename)).execute()
            content_data['thumbnail_file_path'] = "https://drive.google.com/open?id=" + drive_file['id']
          self.contents.append(content_data)
          self.uploaded_contents[content_data["content_id"]] = content_data
    except Exception:
      print("Error"+content.source_id+" "+content_data["content_id"])
      pass

  def process_content(self, content, hierarchy_path):
    hierarchy_path = "{} -> {}".format(hierarchy_path, content.title)

    try:
      if len(self.contents) != 0 and len(self.contents) % 200 == 0:
        with open('khan_contents.json', 'w') as outfile:
          json.dump(self.contents, outfile)
        with open('khan_final.json', 'w') as outfile:
          json.dump(self.uploaded_contents, outfile)

    except Exception:
      print("JSONGenError")
      pass

    if isinstance(content, ricecooker.classes.nodes.TopicNode):
      for child_topic in content.children:
        self.process_content(child_topic, hierarchy_path)
    else:
      self.extract_content(content, hierarchy_path)
  
  def update_thumbnails(self):
    page_token = None
    while True:
      response = self.service.files().list(q="'1K5byDiFyEHqtjSmv36O4_oZjhh3tskAJ' in parents",
                                            spaces='drive',
                                            fields='nextPageToken, files(id, name)',
                                            pageToken=page_token,
                                            pageSize=990).execute()
      print("requested")
      for file in response.get('files', []):
        content = next((x for x in self.contents if x["thumbnail_file_name"] == file['name'] and "thumbnail_file_path" not in x), None)
        if content != None:
          content["thumbnail_file_path"] = "https://drive.google.com/open?id=" + file["id"]
          self.uploaded_contents[content["content_id"]] = content
        # Process change
      page_token = response.get('nextPageToken', None)
      if page_token is None:
        break

  def generate_data(self):
    with open('khan_contents.json', 'w') as outfile:
      json.dump(self.contents, outfile)

    with open('khan_final.json', 'w') as outfile:
      json.dump(self.uploaded_contents, outfile)

    self.contents = []
    for key, val in self.uploaded_contents.items():
      self.contents.append(val)

    with open('contents.csv', 'w') as csvfile:
      # kind, hierarchy_path, title, description, language, thumbnail_file_name, thumbnail_path, drive_file_name, drive_file_path
      fieldnames = ['kind', 'hierarchy_path', 'title', 'description', 'language', "thumbnail_file_name", "thumbnail_file_path", "drive_file_name", "drive_file_path"]
      writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
      writer.writeheader()
      generation = {}
      print('generation')
      for content in self.contents:
        content.pop("questions", None)
        content.pop("tags", None)
        content.pop("extra_fields", None)
        content.pop("node_id", None)
        content["description"] = content["description"].strip()
        if content["kind"] in generation:
          print('.')
        else:
          generation[content["kind"]] = 0

        content.pop("files", None)
        content.pop("thumbnail", None)
        content.pop("copyright_holder", None)
        content.pop("source_domain", None)
        content.pop("author", None)
        content.pop("role", None)
        content.pop("content_id", None)
        content.pop("license", None)
        content.pop("license_description", None)
        content.pop("source_id", None)
        content.pop("license", None)
        writer.writerow(content)
        generation[content["kind"]] += 1;


  def main(self):
    with open('khan_final.json', 'r') as outfile:
      self.uploaded_contents = json.load(outfile)

    for key, val in self.uploaded_contents.items():
      self.contents.append(val)

    pickle_in = open(self.pickle_path,"rb")
    example_dict = pickle.load(pickle_in)

    SCOPES = "https://www.googleapis.com/auth/drive"
    store = file.Storage('credentials.json')
    creds = store.get()
    if not creds or creds.invalid:
      flow = client.flow_from_clientsecrets('client_secret.json', SCOPES)
      creds = tools.run_flow(flow, store)
    self.service = build('drive', 'v3', http=creds.authorize(Http()))

    for index, child_topic in enumerate(example_dict.channel.children):
      # if index == 0:
      #   continue

      self.process_content(child_topic, example_dict.channel.title)
      if index == 2:
        break
    self.filtered = list(filter(lambda x: x["kind"] != 'video', self.contents))
    # pdb.set_trace()
    self.update_thumbnails()
    self.generate_data()


if __name__ == '__main__':
  chef = ReadPickle("restore/82842493ad01f65b5cc518ac7f425c4a/last.pickle")
  chef.main()
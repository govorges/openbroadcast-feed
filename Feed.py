import xml.etree.ElementTree as ET
from Bunny import BunnyAPI
from os import path, environ

import psycopg2
import time

postgres_connection = None
while postgres_connection is None:
    try:
        postgres_connection = psycopg2.connect(
            database=environ["POSTGRESDB_DATABASE"],
            host=environ["POSTGRESDB_HOST"],
            user=environ["POSTGRESDB_USER"],
            password=environ["POSTGRES_PASSWORD"],
            port=environ["POSTGRESDB_DOCKER_PORT"]
        )
    except:
        time.sleep(10)

postgres_cursor = postgres_connection.cursor()

HOME_DIR = path.dirname(path.realpath(__file__))

class VideoObject:
    def __init__(self, video_id: str, metadata: dict, date_created, date_modified):
        self.video_id = video_id
        self.metadata = metadata
        self.date_created = date_created
        self.date_modified = date_modified

class FeedManager:
    def __init__(self):
        self.bunny = BunnyAPI()

    def db__RetrieveQueryDataFromVideoID(self, video_id: str):
        '''Retrieves a video object from the PostgresDB by the `video_id` column value.'''
        postgres_cursor.execute('SELECT * FROM public."Videos" WHERE video_id = %(video_id)s', {'video_id': video_id}) # escaped properly

        queryData = postgres_cursor.fetchone()
        return queryData
    
    def db__RetrieveMostRecent(self, querySize: int):
        '''Returns a List of Video row data of length `querySize` sorted by date_created (newest -> oldest)'''
        postgres_cursor.execute(f'SELECT * FROM public."Videos" ORDER BY date_created DESC LIMIT {querySize}') # strict int

        data = postgres_cursor.fetchall()
        if len(data) > querySize:
            querySize = len(data)
        
        data = data[:querySize]

        return data
    
    def internal__GetStoredFileListFromBunny(self, path):
        '''Returns a List of filenames stored in Bunny at the given path.'''
        bunny_fileList = self.bunny.file_List(path=path)
        
        filenames = []
        
        for item in bunny_fileList:
            filenames.append(item.get("ObjectName").rsplit(".", 1)[0])
        return filenames

    def video_CreateFromID(self, video_id: str):
        queryData = self.db__RetrieveQueryDataFromVideoID(video_id=video_id)
        return self.video_CreateFromQueryData(queryData)

    def video_CreateFromQueryData(self, queryData):
        videoObj = VideoObject(
            video_id = queryData[0],
            metadata = queryData[1],
            date_created = queryData[2],
            date_modified = queryData[3]
        )
        return videoObj
    
    def video_RetrieveMostRecent(self, querySize: int):
        queryData = self.db__RetrieveMostRecent(querySize = querySize)
        
        videos = []
        for object in queryData:
            video = self.video_CreateFromQueryData(object)
            videos.append(video)
        return videos
    
    def CreateFeed_MostRecentVideos(self):
        videos = self.video_RetrieveMostRecent(querySize=50)

        rss_root = ET.Element("rss")
        rss_root.set("xmlns:irc", "http://gov.org.es/")
        rss_root.set("version", "2.0")

        channel_Element = ET.SubElement(rss_root, "channel")

        grt_El = ET.SubElement(channel_Element, "irc:gridrowtitle")
        grt_El.text = "Most Recent Uploads"

        for video in videos:
            if video.metadata.get("feedTags") is None:
                continue
            item = ET.SubElement(channel_Element, "item")

            for key in video.metadata["feedTags"].keys():
                subElement = ET.SubElement(item, f"irc:{key}")
                subElement.text = str(video.metadata["feedTags"][key])

        output_xml = ET.tostring(rss_root)
        with open(path.join(HOME_DIR, "uploads", "MostRecent.xml"), "wb") as fileW:
            fileW.write(output_xml)
            fileW.flush()

        self.bunny.file_Delete("/feeds/MostRecent.xml")
        self.bunny.file_QueueUpload(
            local_file_path = path.join(HOME_DIR, "uploads", "MostRecent.xml"),
            target_file_path = "/feeds/MostRecent.xml"
        )
        


if __name__ == "__main__":
    fm = FeedManager()
    fm.CreateFeed_MostRecentVideos()
import csv
from hdfs import InsecureClient
from flask import current_app as app


class CSVWriter:
    def __init__(self):
        self.hdfs_client = InsecureClient(app.config['HDFS_URL'], user='hdfs')
        self.directory_path = app.config['HDFS_DIRECTORY_PATH']
        self.file_path = f"""{self.directory_path}{app.config['HDFS_CSV_PATH']}"""
        self.headers = ['userId', 'movieId', 'rating']

        self._ensure_hdfs_directory_exists()
        self._ensure_file_headers()

    def _ensure_hdfs_directory_exists(self):
        try:
            if not self.hdfs_client.content(self.directory_path, strict=False):
                self.hdfs_client.makedirs(self.directory_path)
                print(f"Directory {self.directory_path} created successfully.")
            else:
                print(f"Directory {self.directory_path} already exists.")
        except Exception as e:
            print(f"An error occurred while ensuring directory existence: {e}")

    def _ensure_file_headers(self):
        try:
            with self.hdfs_client.read(self.file_path, encoding='utf-8') as reader:
                file_content = reader.read()
            if not file_content.strip():
                with self.hdfs_client.write(self.file_path, encoding='utf-8', overwrite=True) as writer:
                    writer.write("userId,movieId,rating\n")
                    print("Headers written to the HDFS file.")
        except Exception as e:
            if 'File ' + self.file_path + ' not found' in str(e):
                with self.hdfs_client.write(self.file_path, encoding='utf-8', overwrite=True) as writer:
                    writer.write("userId,movieId,rating\n")
                    print(f"File {self.file_path} created successfully Headers written to the HDFS file.")
            else:
                print(f"An error occurred while ensuring file headers: {e}")

    def write_to_csv(self, data):
        try:
            with self.hdfs_client.write(self.file_path, append=True, encoding='utf-8') as writer:
                csv_writer = csv.DictWriter(writer, fieldnames=self.headers)
                for row in data:
                    csv_writer.writerow(row)
                    print(f"row added successfully {row}.")
        except Exception as e:
            print(f"An error occurred while writing to HDFS: {e}")

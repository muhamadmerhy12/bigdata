import csv
import os


class CSVWriter:
    def __init__(self, filename, headers):
        self.filename = filename
        self.headers = headers
        self.file_exists = os.path.isfile(self.filename)

    def write_to_csv(self, data):
        with open(self.filename, mode='a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=self.headers)
            if not self.file_exists:
                writer.writeheader()
                self.file_exists = True
            for row in data:
                writer.writerow(row)

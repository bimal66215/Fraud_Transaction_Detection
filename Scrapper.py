import pickle
import urllib
import requests
from bs4 import BeautifulSoup
import pandas as pd
from application_logging import logger
from tqdm import tqdm

class Scrapper:

    '''

    This Function gets the Data from the Git repo provided. The Files are stored as Batches for everyday.

    '''

    def __init__(self):
        self.repo_ur = "https://github.com/Fraud-Detection-Handbook/simulated-data-raw/tree/main/data"
        self.log_file_object = open("Scrapper_logs/scrapperlog.txt", 'a+')
        self.log_writer = logger.App_Logger()


    def get_data(self):

        try:

            self.log_writer.log(self.log_file_object, 'Start of Data Scrapping!!')

            # Empty list for all the downloadable links and the Output File paths
            download_url_list = []
            out_paths = []

            # Getting the HTML text from the Repo URL
            url = "https://github.com/Fraud-Detection-Handbook/simulated-data-raw/tree/main/data"
            req1 = requests.get(url).text
            soup = BeautifulSoup(req1)

            # Getting the File Links from the Repo

            self.log_writer.log(self.log_file_object, 'Started getting the File downloadable links!!')

            for file_link in tqdm(soup.find_all('a', attrs="js-navigation-open Link--primary" )):

                # making the Complete URL from 'href' in the HTML
                file_url = "https://github.com/" + file_link.get('href')

                # Getting the HTML code as text
                req2 = requests.get(file_url).text

                # Using BeautifulSoup to get all the Data Under the 'a' Tag in the HTMl text
                soup2 = BeautifulSoup(req2)
                for down_link in tqdm(soup2.find_all('a')):

                    # Getting the all the 'href' links with the "View raw" string
                    if down_link.string == "View raw":

                        # Adding the Urls to the "download_url_list" for the .pkl files to be downloaded
                        download_url_list.append("https://github.com/" + down_link.get('href'))



            self.log_writer.log(self.log_file_object, 'Finished getting the File downloadable links!!')

            self.log_writer.log(self.log_file_object, 'Started Downloading the .pkl Files from the links!!')



            for i in tqdm(download_url_list):

                # Getting the Name of the File from the URL
                name = i.rsplit("/", 1)[1].rsplit(".pkl")[0]

                # Opening the URL
                f = urllib.request.urlopen(i)

                # Loading the Pickle file
                data = pickle.load(f)

                # Generating the Output File path and appendign it to the "out_paths" list

                out_file_path = "Training_Batch_Files/"+name +".csv"
                out_paths.append(out_file_path)

                # Importing as CSV
                data.to_csv(out_file_path)

            self.log_writer.log(self.log_file_object, 'Finished Downloading the .pkl Files from the links!!')

            self.log_writer.log(self.log_file_object, 'End of Data Scrapping!!')

            return out_paths


        except Exception as e:
            self.log_writer.log(self.log_file_object, str(e))
            raise e



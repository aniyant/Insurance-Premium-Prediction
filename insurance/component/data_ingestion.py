from insurance.exception import InsuranceException
from insurance.logger import logging
from insurance.entity.artifact_entity import DataIngestionArtifact,DatabaseArtifact
from insurance.entity.config_entity import DataIngestionConfig, DatabaseConfig

import pandas as pd
import numpy as np
from six.moves import urllib
import mysql.connector as mysql_connect
import sys,os
import gdown

from sklearn.model_selection import StratifiedShuffleSplit

class DataIngestion:

    def __init__(self,data_ingestion_config:DataIngestionConfig, database_config:DatabaseConfig ):
        try:
            logging.info(f"{'>>'*20}Data Ingestion log started.{'<<'*20} ")
            self.data_ingestion_config = data_ingestion_config
            self.database_config = database_config
        
        except Exception as e:
            raise InsuranceException(e,sys)
    

    def download_insurance_data(self,) -> str:
        try:
            #extraction remote url to download dataset
            download_url = self.data_ingestion_config.dataset_download_url

            #folder location to download file
            raw_download_dir = self.data_ingestion_config.raw_data_dir
            
            os.makedirs(raw_download_dir,exist_ok=True)

            if "drive.google.com" in download_url:
                raw_data_file_path = os.path.join(raw_download_dir,'insurance.csv')
            else:
                insurance_file_name = os.path.basename(download_url)
                raw_data_file_path = os.path.join(raw_download_dir, insurance_file_name)

            logging.info(f"Downloading file from :[{download_url}] into :[{raw_data_file_path}]")
            if 'drive.google.com' in download_url:
                gdown.download(download_url,raw_data_file_path,quiet=False)
            else:
                urllib.request.urlretrieve(download_url, raw_data_file_path)
            logging.info(f"File :[{raw_data_file_path}] has been downloaded successfully.")

        except Exception as e:
            raise InsuranceException(e,sys) from e

    def save_data_into_database(self) -> DatabaseArtifact:
        try:
            host = self.database_config.database_host
            username = self.database_config.database_username
            password = self.database_config.database_password
            database_name = self.database_config.database_name
            table_name = self.database_config.table_name

            db_connect = mysql_connect.connect(host=host,username=username,password=password)
            db_cursor = db_connect.cursor()

            db_cursor.execute(f'CREATE DATABASE IF NOT EXISTS {database_name}')
            db_cursor.execute(f'USE {database_name}')
            
            query = f"""CREATE TABLE IF NOT EXISTS {table_name}(id INT AUTO_INCREMENT PRIMARY KEY, age INT(10), 
                        sex VARCHAR(10), bmi FLOAT(10,3), children INT(5), smoker VARCHAR(10), region VARCHAR(40), 
                        expenses FLOAT(20,3))"""
            db_cursor.execute(query)

            # Locating the file which has been downloaded 
            raw_data_dir = self.data_ingestion_config.raw_data_dir
            file_name = os.listdir(raw_data_dir)[0]
            insurance_file_path = os.path.join(raw_data_dir,file_name)
            logging.info(f"insurance file path : {insurance_file_path}")
            # Reading the file
            insurance_dataframe = pd.read_csv(insurance_file_path)
            count = 0
            db_cursor.execute('show tables')
            if table_name in db_cursor.fetchall()[0]:
                for row in insurance_dataframe.to_numpy():
                    query = f"INSERT INTO {table_name}(age,sex,bmi,children,smoker,region,expenses) VALUES({int(row[0])},'{str(row[1])}',{float(row[2])},{int(row[3])},'{str(row[4])}','{str(row[5])}',{float(row[6])})"
                    db_cursor.execute(query)
                    count = count +1
            else:
                logging.info(f"table: {table_name} not found in database:{database_name}")
            logging.info(f"row affected or saved in database :{count}")
            db_connect.commit()
            db_connect.close()

            database_artifact=DatabaseArtifact(database_host=host,
                                                database_username=username,
                                                database_password=password,
                                                database_name=database_name,
                                                table_name=table_name)
        
            logging.info(f"Data saved in database. DatabaseArtifact:{database_artifact}")
        except Exception as e:
            raise InsuranceException(e,sys) from e
    
    def split_data_as_train_test(self) -> DataIngestionArtifact:
        try:
            raw_data_dir = self.data_ingestion_config.raw_data_dir

            file_name = os.listdir(raw_data_dir)[0]

            insurance_file_path = os.path.join(raw_data_dir,file_name)


            logging.info(f"Reading csv file: [{insurance_file_path}]")
            insurance_data_frame = pd.read_csv(insurance_file_path)

            insurance_data_frame["expenses_category"] = pd.cut(
                insurance_data_frame["expenses"],
                bins=[0.0, 1.5, 3.0, 4.5, 6.0, np.inf],
                labels=[1,2,3,4,5]
            )
            

            logging.info(f"Splitting data into train and test")
            strat_train_set = None
            strat_test_set = None

            split = StratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=42)

            for train_index,test_index in split.split(insurance_data_frame, insurance_data_frame["expenses_category"]):
                strat_train_set = insurance_data_frame.loc[train_index].drop(["expenses_category"],axis=1)
                strat_test_set = insurance_data_frame.loc[test_index].drop(["expenses_category"],axis=1)

            train_file_path = os.path.join(self.data_ingestion_config.ingested_train_dir,
                                            file_name)

            test_file_path = os.path.join(self.data_ingestion_config.ingested_test_dir,
                                        file_name)
            
            if strat_train_set is not None:
                os.makedirs(self.data_ingestion_config.ingested_train_dir,exist_ok=True)
                logging.info(f"Exporting training datset to file: [{train_file_path}]")
                strat_train_set.to_csv(train_file_path,index=False)

            if strat_test_set is not None:
                os.makedirs(self.data_ingestion_config.ingested_test_dir, exist_ok= True)
                logging.info(f"Exporting test dataset to file: [{test_file_path}]")
                strat_test_set.to_csv(test_file_path,index=False)
            

            data_ingestion_artifact = DataIngestionArtifact(train_file_path=train_file_path,
                                test_file_path=test_file_path,
                                is_ingested=True,
                                message=f"Data ingestion completed successfully."
                                )
            logging.info(f"Data Ingestion artifact:[{data_ingestion_artifact}]")
            return data_ingestion_artifact

        except Exception as e:
            raise InsuranceException(e,sys) from e

    def initiate_data_ingestion(self)-> DataIngestionArtifact:
        try:
            self.download_insurance_data()
            self.save_data_into_database()
            return self.split_data_as_train_test()
        except Exception as e:
            raise InsuranceException(e,sys) from e
    


    def __del__(self):
        logging.info(f"{'>>'*20}Data Ingestion log completed.{'<<'*20} \n\n")

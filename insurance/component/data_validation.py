from insurance.logger import logging
from insurance.exception import InsuranceException
from insurance.entity.config_entity import DataValidationConfig
from insurance.entity.artifact_entity import DataIngestionArtifact,DataValidationArtifact
from insurance.util.util import read_yaml_file
from insurance.constant import DATASET_COLUMNS_KEY, DATASET_DOMAIN_VALUE_KEY

import os,sys
import pandas  as pd
from evidently.model_profile import Profile
from evidently.model_profile.sections import DataDriftProfileSection
from evidently.dashboard import Dashboard
from evidently.dashboard.tabs import DataDriftTab
import json

class DataValidation:
    

    def __init__(self, data_validation_config:DataValidationConfig,
        data_ingestion_artifact:DataIngestionArtifact):
        try:
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
        except Exception as e:
            raise InsuranceException(e,sys) from e


    def get_train_and_test_df(self):
        try:
            train_df = pd.read_csv(self.data_ingestion_artifact.train_file_path)
            test_df = pd.read_csv(self.data_ingestion_artifact.test_file_path)
            return train_df,test_df
        except Exception as e:
            raise InsuranceException(e,sys) from e


    def is_train_test_file_exists(self)->bool:
        try:
            logging.info("Checking if training and test file is available")
            is_train_file_exist = False
            is_test_file_exist = False

            train_file_path = self.data_ingestion_artifact.train_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            is_train_file_exist = os.path.exists(train_file_path)
            is_test_file_exist = os.path.exists(test_file_path)

            is_available =  is_train_file_exist and is_test_file_exist

            logging.info(f"Is train and test file exists?-> {is_available}")
            
            if not is_available:
                training_file = self.data_ingestion_artifact.train_file_path
                testing_file = self.data_ingestion_artifact.test_file_path
                message=f"Training file: {training_file} or Testing file: {testing_file}" \
                    "is not present"
                raise Exception(message)

            return is_available
        except Exception as e:
            raise InsuranceException(e,sys) from e

    
    def validate_dataset_schema(self)->bool:
        try:
            validation_status = False
            train_file_path = self.data_ingestion_artifact.train_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            train_df = pd.read_csv(train_file_path)
            test_df = pd.read_csv(test_file_path)

            schema_file_path = self.data_validation_config.schema_file_path
            dataset_schema_info = read_yaml_file(file_path=schema_file_path)

            #validate training and testing dataset using schema file
            #1. Number of Column
            valid_number_of_columns = False

            if len(train_df.columns) == len(test_df.columns):
                valid_number_of_columns = True

            logging.info(f"validation of number of columns in train and test  data is : {valid_number_of_columns}")    

            #2. Check column names
            valid_columns_names = False

            for col1 in train_df.columns:
                for col2 in test_df.columns:
                    if col1 == col2:
                        if col1 in dataset_schema_info[DATASET_COLUMNS_KEY].keys():
                            valid_columns_names = True

            logging.info(f"validation names of columns in train and test data is :{valid_columns_names}")

            #2. Check the domain value of the categorical dataset
            # train dataset domain check 
            valid_train_domain_value = 0

            for column in train_df.columns:
                if column in dataset_schema_info[DATASET_DOMAIN_VALUE_KEY].keys():
                    category = train_df[column].value_counts().to_dict().keys()
                    for cat in category:
                        if cat not in dataset_schema_info[DATASET_DOMAIN_VALUE_KEY][column]:
                            valid_train_domain_value += 1
                            logging.info(f"train data :- domain value: {cat} not match with any schema domain {column} value.")
                            
            if valid_train_domain_value != 0:
                valid_train_domain_value = False
            else:
                valid_train_domain_value = True

            logging.info(f"Train data :- valid domain value :{valid_train_domain_value}")

            # test dataset domain check
            valid_test_domain_value = 0
            for column in test_df.columns:
                if column in dataset_schema_info[DATASET_DOMAIN_VALUE_KEY].keys():
                    category = test_df[column].value_counts().to_dict().keys()
                    for cat in category:
                        if cat not in dataset_schema_info[DATASET_DOMAIN_VALUE_KEY][column]:
                            valid_test_domain_value += 1
                            logging.info(f"test data :- domain value: {cat} not match with any schema domain {column} value.")
            
            if valid_test_domain_value != 0:
                valid_test_domain_value = False
            else:
                valid_test_domain_value = True

            logging.info(f"Test data :- valid domain value :{valid_test_domain_value}")
            
            if valid_number_of_columns and valid_columns_names \
                and valid_train_domain_value and valid_test_domain_value:
                validation_status = True
            
            return validation_status  
        except Exception as e:
            raise InsuranceException(e,sys) from e

    def get_and_save_data_drift_report(self):
        try:
            profile = Profile(sections=[DataDriftProfileSection()])

            train_df,test_df = self.get_train_and_test_df()

            profile.calculate(train_df,test_df)

            report = json.loads(profile.json())

            report_file_path = self.data_validation_config.report_file_path
            report_dir = os.path.dirname(report_file_path)
            os.makedirs(report_dir,exist_ok=True)

            with open(report_file_path,"w") as report_file:
                json.dump(report, report_file, indent=6)
            return report
        except Exception as e:
            raise InsuranceException(e,sys) from e

    def save_data_drift_report_page(self):
        try:
            dashboard = Dashboard(tabs=[DataDriftTab()])
            train_df,test_df = self.get_train_and_test_df()
            dashboard.calculate(train_df,test_df)

            report_page_file_path = self.data_validation_config.report_page_file_path
            report_page_dir = os.path.dirname(report_page_file_path)
            os.makedirs(report_page_dir,exist_ok=True)

            dashboard.save(report_page_file_path)
        except Exception as e:
            raise InsuranceException(e,sys) from e

    def is_data_drift_found(self)->bool:
        try:
            report = self.get_and_save_data_drift_report()
            self.save_data_drift_report_page()
            return True
        except Exception as e:
            raise InsuranceException(e,sys) from e

    def initiate_data_validation(self)->DataValidationArtifact :
        try:
            logging.info(f"{'>>'*30}Data Valdaition log started.{'<<'*30} \n\n")

            self.is_train_test_file_exists()
            self.validate_dataset_schema()
            self.is_data_drift_found()
            logging.info("Data Drift report saved.")

            data_validation_artifact = DataValidationArtifact(
                schema_file_path=self.data_validation_config.schema_file_path,
                report_file_path=self.data_validation_config.report_file_path,
                report_page_file_path=self.data_validation_config.report_page_file_path,
                is_validated=True,
                message="Data Validation performed successully."
            )
            logging.info(f"Data validation artifact: {data_validation_artifact}")
            return data_validation_artifact
        except Exception as e:
            raise InsuranceException(e,sys) from e


    def __del__(self):
        logging.info(f"{'>>'*30}Data Valdaition log completed.{'<<'*30} \n\n")
        
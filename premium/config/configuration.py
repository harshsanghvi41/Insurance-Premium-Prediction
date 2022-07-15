from premium.exception import PremiumException
from premium.logger import logging
from premium.util.util import read_yaml_file
from premium.entity.config_entity import *
from premium.constant import *
import os,sys

class Configuration:
    def __init__(self, 
                 config_file_path = CONFIG_FILE_PATH, 
                 current_time_stamp = CURRENT_TIME_STAMP) -> None:
        try:
            self.config_info = read_yaml_file(file_path = config_file_path)
            self.training_pipeline_config = self.get_training_pipeline_config()
            self.time_stamp = current_time_stamp

        except Exception as e:
            raise Exception(e,sys) from e

    
    def get_data_ingestion_config(self) -> DataIngestionConfig:
        try:
            artifact_dir = self.training_pipeline_config.artifact_dir

            data_ingestion_artifact_dir = os.path.join(artifact_dir, 
                                                       DATA_INGESTION_ARTIFACT_DIR, 
                                                       self.time_stamp)

            data_ingestion_info = self.config_info[DATA_INGESTION_CONFIG_KEY]

            dataset_download_url = data_ingestion_info[DATA_INGESTION_DOWNLOAD_URL_KEY]

            tgz_download_dir = os.path.join(data_ingestion_artifact_dir, 
                                            data_ingestion_info[DATA_INGESTION_TGZ_DOWNLOAD_DIR_KEY])

            raw_data_dir = os.path.join(data_ingestion_artifact_dir,
                                        data_ingestion_info[DATA_INGESTION_RAW_DATA_DIR])

            ingested_data_dir = os.path.join(data_ingestion_artifact_dir,
                                             data_ingestion_info[DATA_INGESTION_INGESTED_DIR_NAME_KEY])

            ingested_train_dir = os.path.join(ingested_data_dir,
                                              data_ingestion_info[DATA_INGESTION_INGESTED_TRAIN_DIR_KEY])

            ingested_test_dir = os.path.join(ingested_data_dir,
                                             data_ingestion_info[DATA_INGESTION_INGESTED_TEST_DIR_KEY])

            data_ingestion_config = DataIngestionConfig(
                dataset_download_url = dataset_download_url,
                tgz_download_dir = tgz_download_dir,
                raw_data_dir = raw_data_dir,
                ingested_train_dir = ingested_train_dir,
                ingested_test_dir = ingested_test_dir
            )

            logging.info(f"Data Ingestion config: {data_ingestion_config}")
            return data_ingestion_config

        except Exception as e:
            raise PremiumException(e,sys) from e


    def get_data_validation_pipelile_config(self) -> DataValidationConfig:
        try:
            artifact_dir = self.training_pipeline_config.artifact_dir

            data_validation_artifact_dir = os.path.join(artifact_dir,
                                                    DATA_VALIDATION_ARTIFACT_DIR_NAME,
                                                    self.time_stamp)

            data_validation_config = self.config_info[DATA_VALIDATION_CONFIG_KEY]

            schema_file_path = os.path.join(ROOT_DIR,
                                            data_validation_config[DATA_VALIDATION_SCHEMA_DIR_KEY],
                                            data_validation_config[DATA_VALIDATION_SCHEMA_FILE_NAME_KEY])

            report_file_path = os.path.join(data_validation_artifact_dir,
                                            data_validation_config[DATA_VALIDATION_REPORT_FILE_NAME_KEY])

            report_page_file_path = os.path.join(data_validation_artifact_dir,
                                                 data_validation_config[DATA_VALIDATION_REPORT_PAGE_FILE_NAME_KEY])

            data_validation_config = DataValidationConfig(schema_file_path = schema_file_path,
                                                          report_file_path = report_file_path,
                                                          report_page_file_path = report_page_file_path)

            return data_validation_config

        except Exception as e:
            raise PremiumException(e,sys) from e


    def get_data_transformation_config(self) -> DataTransformationConfig:
        try:
            pass

        except Exception as e:
            raise PremiumException(e,sys) from e

    
    def get_model_trainer_config(self) -> ModelTrainerConfig:
        try:
            pass

        except Exception as e:
            raise PremiumException(e,sys) from e


    def get_model_evaluation_config(self) -> ModelEvaluationConfig:
        try:
            pass

        except Exception as e:
            raise PremiumException(e,sys) from e


    def get_model_pusher_config(self) -> ModelPusherConfig:
        try:
            pass

        except Exception as e:
            raise PremiumException(e,sys) from e


    def get_training_pipeline_config(self) -> TrainingPipelineConfig:
        try:
            training_pipeline_config = self.config_info[TRAINING_PIPELINE_CONFIG_KEY]
            artifact_dir = os.path.join(ROOT_DIR,
            training_pipeline_config[TRAINING_PIPELINE_NAME_KEY],
            training_pipeline_config[TRAINING_PIPELINE_ARTIFACT_DIR_KEY]
            )

            training_pipeline_config = TrainingPipelineConfig(artifact_dir=artifact_dir)
            logging.info(f"Training pipleine config: {training_pipeline_config}")
            return training_pipeline_config

        except Exception as e:
            raise PremiumException(e,sys) from e

    
    
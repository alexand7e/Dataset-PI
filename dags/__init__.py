from dags.estruturas import InformationManager
from dags.operadores import GoogleSheetManager, GoogleDriveManager
from dags.sidrapi import SidraManager
from dags.configure_directory import DirectoryManager
from dags.configure_dag import (configure_paths,
                                configure_managers,
                                get_sidra_api_info,
                                get_sidra_api_data,
                                configure_drive_repository)
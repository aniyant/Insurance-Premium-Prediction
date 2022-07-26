from setuptools import setup,find_packages
from typing import List

#Declaring variables for setup functions
PROJECT_NAME="Insurance-Premium-Predictor"
VERSION="0.0.1"
AUTHOR="Sunny Kumar"
DESRCIPTION="This is a Insurance Premium Prediction Project taken as intership project for my FSDS course."
PACKAGES=["insurance"]
REQUIREMENT_FILE_NAME="requirements.txt"


def get_requirements_list()->List[str]:
    """
    Description: This function is going to return list of requirement 
    mention in requirements.txt file
    return This function is going to return a list which contain name 
    of libraries mentioned in requirements.txt file
    """
    with open(REQUIREMENT_FILE_NAME) as requirement_file:
        return requirement_file.readlines().remove("-e .")
        #return requirement_file.readlines()


setup(
name=PROJECT_NAME,
version=VERSION,
author=AUTHOR,
description=DESRCIPTION,
packages=find_packages(),   # ["insurance"]
install_requires=get_requirements_list()
)
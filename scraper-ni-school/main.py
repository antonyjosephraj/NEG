import os
import time
import zipfile
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from tempfile import mkdtemp
from botocore.exceptions import ClientError
import boto3
import pandas as pd

import json

# Getting Credentials
secret_name = "NEGSourceCredentials"
region_name = "eu-west-2"

# Create a Secrets Manager client
session = boto3.session.Session()
client = session.client( service_name='secretsmanager',region_name=region_name)
try:
    response = client.get_secret_value(SecretId=secret_name)
    # Decrypts secret using the associated KMS key.
    secret_value = json.loads(response['SecretString'])
except Exception as e:
    print(e)

# S3 bucket configuration
s3 = boto3.resource('s3',
                     aws_access_key_id = secret_value['AWS_ACCESS_KEY_ID_S3'], 
                     aws_secret_access_key = secret_value['AWS_SECRET_KEY_S3'],
                     region_name='eu-west-2')

# set the bucket name and key (i.e., file name) for the CSV file
bucket_name = os.environ['BUCKET_NAME'] #'ni-extract'
source = os.environ['SOURCE']
file_name = os.environ['FILE_NAME'] #'ni-source/ni_extract_schools.csv'
key = source+"/"+file_name+".csv"

def handler(event=None, context=None):
    try:
        print("Launching browser")
        # Change the directory to temporary
        temp_dir = os.chdir('/tmp')
        print(temp_dir)

        # Initiating chrome
        options = webdriver.ChromeOptions()
        options.binary_location = '/opt/chrome/chrome'
        options.add_experimental_option("excludeSwitches", ["ignore-certificate-errors"])
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1280x1696")
        options.add_argument("--single-process")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-dev-tools")
        options.add_argument("--no-zygote")
        options.add_argument(f"--user-data-dir={temp_dir}")
        options.add_argument(f"--data-path={temp_dir}")
        options.add_argument(f"--disk-cache-dir={temp_dir}")
        options.add_argument("--remote-debugging-port=9222")
        driver = webdriver.Chrome("/opt/chromedriver",options=options)
        driver.wait = WebDriverWait(driver, 100)
        driver.get("http://apps.education-ni.gov.uk/appinstitutes/default.aspx")
        time.sleep(3)

    # Select the export option
        driver.find_element("xpath",'//*[@id="accordian2"]/div/div[1]/h4/a').click()
        time.sleep(3)
        print("Export option clicked")

        # Download the excel file
        driver.find_element("xpath",'//*[@id="ContentPlaceHolder1_lvSchools_btnDoExport"]').click()
        time.sleep(5)
        print("Excel file is downloaded")

        current_dir = os.getcwd()
        files = os.listdir(current_dir)
        files = sorted(files)
        for file in files:
            if file.endswith('.xlsx'):
                c_file = current_dir+"/"+file
                data = pd.read_excel(c_file)
                try:
                    # convert the DataFrame to a CSV string
                    csv_string = data.to_csv(index=False).encode('utf-8')

                    # Move file to S3
                    s3.Bucket(bucket_name).put_object(Key=key, Body=csv_string)
                    print("file moved to s3")

                    # Remove the directory in temporary location
                    os.remove(c_file)
                except Exception as e:
                    print("S3 file error: ",e) 
        
        # Close the driver
        driver.close()
        driver.quit()
        print("Process Completed")
        response = {"statusCode": 200,"body": "NI data successfully extracted and stored in S3"}

        return response
    except Exception as e:
        return {'status':False, 'message':str(e)}


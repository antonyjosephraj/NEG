import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from tempfile import mkdtemp
from botocore.exceptions import ClientError
import boto3
import pandas as pd
from zipfile import ZipFile


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
bucket_name = os.environ['BUCKET_NAME']
source = os.environ['SOURCE']
file_name = os.environ['FILE_NAME'] 
key = source+"/"+file_name+".csv"

def handler(event=None, context=None):
    
    try:    
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
        driver.get('https://www.get-information-schools.service.gov.uk/Downloads')

    # Selected establishment links checkbox
        # 0 - Edubase Fields
        # 1 - Edubase Links
        # 9 - Edubase Groups
        path = '//*[@id="Downloads['+os.environ["SELECTOR"]+'].Selected"]'
        driver.find_element("xpath",path).click()
        print('Selected Establishment links CSV')
        time.sleep(10)

        # Clicked download selected files button
        driver.find_element("xpath",'//*[@id="downloadSelected"]').click()
        print('Clicked Download selected files')
        time.sleep(10)

        # Clicked download results zip file button
        driver.find_element("xpath",'//*[@class="govuk-form-group"]/p/*[@class="govuk-button"]').click()
        print('Clicked Download results zip file')
        time.sleep(10)

        print('Zip file downloaded successfully')
        
        current_dir = os.getcwd()

        print('Current directory path',current_dir)
        print('Listed the current directory files or directories',os.listdir(current_dir))
        
        current_dir = os.getcwd()
        file = os.listdir(current_dir)
        print(file)
        try:
            # Open the zip file in read mode
            with ZipFile('extract.zip', 'r') as zip:
                # Get a list of all the files in the zip
                file_names = zip.namelist()
                # Loop through each file in the zip
                for file_name in file_names:  
                    # Check if the file is a CSV
                    if file_name.endswith('.csv'):     
                        # Open the file in memory as a bytes object
                        with zip.open(file_name) as file:         
                            # Read the CSV file into a Pandas DataFrame
                            df = pd.read_csv(file,encoding = 'latin-1')
                            try:
                                # convert the DataFrame to a CSV string
                                csv_string = df.to_csv(index=False).encode('utf-8')

                                # Move file to S3
                                s3.Bucket(bucket_name).put_object(Key=key, Body=csv_string)
                                print("file moved to s3")
                            except Exception as e:
                                print("S3 file error: ",e) 
        except Exception as e:
            print(e)

        # Close the driver
        driver.close()
        driver.quit()

        response = {"statusCode": 200,"body": "Edubase data successfully extracted and stored in S3"}

        return response
    except Exception as e:
        print(e)
        return {"statusCode": False,'body':str(e)}

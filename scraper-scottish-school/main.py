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
import json
import pandas as pd
from simpledbf import Dbf5

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

def remove_folder(sub_file):
    # Remove all files within the directory
    for filename in os.listdir(sub_file):
        file_path = os.path.join(sub_file, filename)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
            
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')
    print("All the file removed")

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
        driver.get("https://www.data.gov.uk/dataset/9a6f9d86-9698-4a5d-a2c8-89f3b212c52c/scottish-school-roll-and-locations")
        time.sleep(3)
        try:
            driver.find_element("xpath",os.environ['XPATH']).click()#'//*[@id="content"]/div/div/div/section/table/tbody/tr[1]/td[1]/a'
            time.sleep(5)
            print("Export option clicked")
        except Exception as e:
            print("Xpath error:",e)

        current_dir = os.getcwd()
        
        zip_files = [f for f in os.listdir(current_dir) if f.endswith('.zip')]

        for zip_file in zip_files:
            print(zip_file)
            try:
                with zipfile.ZipFile(zip_file, "r") as zip_ref:
                    zip_ref.extractall()
                    time.sleep(4)
                    print(os.listdir(current_dir))
                # zipfile.extractall(path=temp_dir)
                print("file is extracted")
            except Exception as e:
                print("Error:",e)

        print('current_dir:',current_dir)
        folders = next(os.walk(current_dir))[1]
        print("folders:",folders)
        
        for folder in folders:  
            if folder.startswith('SG_SchoolRoll'):
                sub_file = current_dir+"//"+folder
                print(sub_file)
                for s_file in os.listdir(sub_file):
                    print("s_file: ",s_file)
                    if s_file.endswith('.dbf'):
                        df = Dbf5(sub_file+'/'+s_file).to_dataframe()
                        try:
                            # convert the DataFrame to a CSV string
                            csv_string = df.to_csv(index=False).encode('utf-8')

                            # Move file to S3
                            s3.Bucket(bucket_name).put_object(Key=key, Body=csv_string)
                            print("file moved to s3")
                        except Exception as e:
                            print("S3 file error: ",e) 
                # Remove the Folder:
                remove_folder(sub_file)
                try:
                    zip_file_path = os.path.join(current_dir, zip_file)
                    os.remove(zip_file_path)
                    print("Folder removed")
                except Exception as e:
                    print("Folder Error:",e)
    
        response = {"statusCode": 200,"body": "Scottish data successfully extracted and stored in S3"}

        return response
    except Exception as e:
        return {'status':False, 'message':str(e)}


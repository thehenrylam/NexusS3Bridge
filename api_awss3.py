#!/bin/python3

import os
import sys
import subprocess

AWSS3_APPDIR = "/home/ubuntu/S3ClientScript"
AWSS3_SCRIPT = {
    "list": "./scripts/list.py",
    "remove": "./scripts/remove.py",
    "download": "./scripts/download.py",
    "upload": "./scripts/upload.py"
}

def list_s3(repo_alias):
    os.chdir( AWSS3_APPDIR )

    list_cmd = "python3 {list_script} {repo_alias}".format( list_script=AWSS3_SCRIPT["list"], repo_alias=repo_alias )
    list_prc = subprocess.run([list_cmd], shell=True, stdout=subprocess.PIPE)
    list_out = list_prc.stdout.decode('utf-8')

    output = list(filter( None, list_out.split("\n") ))
    return output

def delete_s3(repo_alias, filepath):
    os.chdir( AWSS3_APPDIR )

    delete_cmd = "python3 {delete_script} {repo_alias} {filepath}".format( delete_script=AWSS3_SCRIPT["remove"], repo_alias=repo_alias, filepath=filepath )
    delete_prc = subprocess.run([delete_cmd], shell=True, stdout=subprocess.PIPE)
    delete_out = delete_prc.stdout.decode('utf-8')

    print(delete_out)

    return

def download_s3(repo_alias, filepath, local_filepath):
    os.chdir( AWSS3_APPDIR )

    download_cmd = "python3 {download_script} {repo_alias} {local_filepath} {filepath}" \
            .format( download_script=AWSS3_SCRIPT["download"], repo_alias=repo_alias, local_filepath=local_filepath, filepath=filepath )

    download_prc = subprocess.run([download_cmd], shell=True, stdout=subprocess.PIPE)
    download_out = download_prc.stdout.decode('utf-8')

    print(download_out)

    return

def upload_s3(repo_alias, local_filepath, filepath):
    os.chdir( AWSS3_APPDIR )
    
    upload_cmd = "python3 {upload_script} {repo_alias} {local_filepath} {filepath}" \
            .format( upload_script=AWSS3_SCRIPT["upload"], repo_alias=repo_alias, local_filepath=local_filepath, filepath=filepath)

    upload_prc = subprocess.run([upload_cmd], shell=True, stdout=subprocess.PIPE)
    upload_out = upload_prc.stdout.decode('utf-8')

    print(upload_out)

    return


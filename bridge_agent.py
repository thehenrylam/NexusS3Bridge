#!/bin/python3

import os
import sys
from dateutil import parser
import time
import datetime
import re

import api_nexus
import api_awss3

NEXUS_APPDIR="/home/ubuntu/SimpleNexus/NexusClientScript"
NEXUS_SCRDIR="{}/scripts".format(NEXUS_APPDIR)
NEXUS_CONFIG="{}/client_config.yml".format(NEXUS_APPDIR) 

BRIDGE_APPDIR="/home/ubuntu/NexusS3Bridge"
BRIDGE_FILE_FORCE_SWEEP="{}/command_files/force_sweep.bridge".format(BRIDGE_APPDIR)
BRIDGE_FILE_SHUTDOWN="{}/command_files/shutdown.bridge".format(BRIDGE_APPDIR)

NUMBER_OF_SECONDS_PER_SWEEP = 900

sys.path.append( NEXUS_SCRDIR )

from detailed_list import NexusListHelper

def extract_substring_from_list(input_list, pattern):
    output = []

    for s in input_list:
        rgx_match = re.search( pattern, s )
        if (rgx_match is None):
            print("WARN: Pattern r'{}' can't find a match the following object: {}".format( rgx_extract, obj ))
            continue
        processed_string = rgx_match.group(1)
        output.append(processed_string)

    return output

def get_filetype_by_regex(list_of_files, filetype, with_subname = False):
    dict_output = dict()

    regex_pattern = r'(.*)\.' + filetype
    for fname in list_of_files:
        m = re.match( regex_pattern, fname )
        if (m is None):
            continue
        subname = match.groups()[0] if with_subname else None
        dict_output[ fname ] = subname

    return dict_output

def filter_files_lock(list_of_files):
    # What it does:
    #   - Exclude *.lock files (and their non-lock counterparts)

    list_of_lock_files = dict()

    pattern = r'(.*)\.lock'
    for fname in list_of_files:
        m = re.match( pattern, fname )
        if m is None:
            continue
        list_of_lock_files[ fname ] = match.groups()[0]

    files_to_exclude_by_lock = set( list_of_lock_files.keys() ) | set( list_of_lock_files.values() )

    output = list( set(list_of_files) - files_to_exclude_by_lock )

    return output

def filter_files_deletion(list_of_files):
    dict_deletion_files = get_filetype_by_regex( list_of_files, "delete", True )

    list_relevant_files = set( dict_deletion_files.keys() ) | set( dict_deletion_files.values() )

    output = list( set(list_of_files) - list_relevant_files )

    return output

# list_nexus: Lists the relevant objects from the nexus repo
def list_nexus(repo_alias):
    # Patterns is as follows: http://nexus-url/repository/<nx-reponame>/(extract-filepath-here)
    rgx_extract = r'.*\/repository\/[^\/]*\/(.*)'
    object_list = api_nexus.list_nx(repo_alias)
    output = extract_substring_from_list(object_list, rgx_extract)

    # [ print("> " + o) for o in output ]
    # print("=====")

    return output

# list_awss3: Lists the relevant objects from the S3 repo
def list_awss3(repo_alias):
    # Pattern is as follows: <s3-repo-name>/(extract-filepath-here)
    rgx_extract = r'[^\/]*\/(.*)'
    object_list = api_awss3.list_s3(repo_alias)
    output = extract_substring_from_list(object_list, rgx_extract)

    # [ print("> " + o) for o in output ]
    # print("=====")

    return output

def delete_nexus(repo_alias, filepath):
    api_nexus.delete_nx(repo_alias, filepath)
    return

def download_nexus(repo_alias, filepath, local_filepath):
    api_nexus.download_nx(repo_alias, filepath, local_filepath)
    return

def upload_nexus(repo_alias, local_filepath, filepath):
    api_nexus.upload_nx(repo_alias, local_filepath, filepath)
    return

def delete_s3(repo_alias, filepath):
    api_awss3.delete_s3(repo_alias, filepath)

def download_awss3(repo_alias, filepath, local_filepath):
    api_awss3.download_s3(repo_alias, filepath, local_filepath)
    return

def upload_awss3(repo_alias, local_filepath, filepath):
    api_awss3.upload_s3(repo_alias, local_filepath, filepath)
    return

def transfer_awss3_to_nexus( nx_repo_alias, s3_repo_alias, filepath ):
    # Determine the temporary filepath  
    tmp_hash = str(time.time()).replace('.', '')
    tmp_filepath = "{bridge_appdir}/tmp/{filename}.tmp{tmp_hash}" \
            .format( bridge_appdir=BRIDGE_APPDIR, filename=os.path.basename(filepath), tmp_hash=tmp_hash )
    
    # TMP_FILEPATH <- (AWSS3: filepath)
    download_awss3( s3_repo_alias, filepath, tmp_filepath )

    # TMP_FILEPATH -> (NEXUS: filepath)
    upload_nexus( nx_repo_alias, tmp_filepath, filepath )

    # Clean up the TMP_FILEPATH
    os.remove( tmp_filepath )

    return

def transfer_nexus_to_awss3( nx_repo_alias, s3_repo_alias, filepath ):
    # Determine the temporary filepath
    tmp_hash = str(time.time()).replace('.', '')
    tmp_filepath = "{bridge_appdir}/tmp/{filename}.tmp{tmp_hash}" \
            .format( bridge_appdir=BRIDGE_APPDIR, filename=os.path.basename(filepath), tmp_hash=tmp_hash )
    
    # TMP_FILEPATH <- (NEXUS: filepath)
    download_nexus( nx_repo_alias, filepath, tmp_filepath )

    # TMP_FILEPATH -> (AWSS3: filepath)
    upload_awss3( s3_repo_alias, tmp_filepath, filepath)

    os.remove( tmp_filepath )

    return

def apply_policy( policy, nx_repo_alias, s3_repo_alias ):
    nx_del_obj = list(policy["nx"]["del"])
    s3_del_obj = list(policy["s3"]["del"])
    nx_del_obj.sort()
    s3_del_obj.sort()

    for f in nx_del_obj:
        delete_nexus( nx_repo_alias, f )
    for f in s3_del_obj:
        delete_nexus( s3_repo_alias, f )

    nx_add_obj = list(policy["nx"]["add"])
    s3_add_obj = list(policy["s3"]["add"])
    nx_add_obj.sort()
    s3_add_obj.sort()

    for f in nx_add_obj:
        transfer_awss3_to_nexus( nx_repo_alias, s3_repo_alias, f )
    for f in s3_add_obj:
        transfer_nexus_to_awss3( nx_repo_alias, s3_repo_alias, f )

    return

def filter_nexus_files_to_push(file_list, nx_repo_alias, file_deletion_threshold=300):
    helper = NexusListHelper( NEXUS_CONFIG, nx_repo_alias )

    current_time = datetime.datetime.now()

    json_list = helper.list()

    output = []
    for f in file_list:
        j = helper.filter(json_list, f)
        if len(j) == 0:
            continue
        j = j[0]
        
        j_path = j["path"]
        j_time = parser.parse(j["lastModified"])
        j_delta_sec = current_time.timestamp() - j_time.timestamp()

        if file_deletion_threshold < j_delta_sec:
            continue

        output.append( f )

    return output

def sweep_set_standard_policy(policy, set_nx_objects, set_s3_objects):
    # Perform the standard sweep policy for the nx and s3 objects 
    #   By default: The S3 side is our source of truth.
    #       - We will primarliy change Nexus to match the file structure of S3 (not the other way around)

    # Get the differences between the two lists to get the list of nexus files to delete and acquire
    nx_files_to_delete  = set_nx_objects - set_s3_objects
    nx_files_to_acquire = set_s3_objects - set_nx_objects

    policy["nx"]["del"] = nx_files_to_delete
    policy["nx"]["add"] = nx_files_to_acquire

    return policy 

def sweep_apply_push_policy(policy, set_nx_objects, nx_repo_alias, file_deletion_threshold=300):
    # Apply a policy to push Nexus files to S3
    # If the Nexus file is detected to have been inserted recently,
    # Then do not remove it from Nexus, and push it to S3

    # Determine the list of files that we should push instead of delete
    push_files = set( filter_nexus_files_to_push( list(set_nx_objects), nx_repo_alias, file_deletion_threshold ) )

    policy["nx"]["del"] -= push_files

    policy["s3"]["add"] |= push_files

    return policy

def sweep_apply_delete_policy(policy, set_nx_objects, lock_files):
    # Apply a policy to remove S3 objects by request of Nexus
    # Get the <<file_name>>\.delete file, and request to remove this <<file_name>>
    # The request to remove will be declined if the <<file_name>> within lock_files
    
    dict_deletion_requests = get_filetype_by_regex( list(set_nx_objects), "delete", True )

    files_to_delete = set()
    for k, v in dict_deletion_requests.items():
        # Do not proceed further if either the key/value is in lock_files
        if (v in lock_files) or (k in lock_files):
            continue
        files_to_delete |= {k, v}

    policy["nx"]["del"] |= files_to_delete
    policy["s3"]["del"] |= files_to_delete

    return policy

def sweep_apply_clamp_policy(policy, set_nx_objects, set_s3_objects):
    # Clamp the policy
    #   - Only delete from X if X objects exists
    #   - Only add to X (via Y) if Y objects exists

    policy["nx"]["del"] &= set_nx_objects
    policy["nx"]["add"] &= set_s3_objects

    policy["s3"]["del"] &= set_s3_objects
    policy["s3"]["add"] &= set_nx_objects

    return policy

def report_policy(policy, nx_repo_alias, s3_repo_alias):
    print("Sweep Delta:")
    for category in policy.keys():
        set_add = policy[ category ]["add"]
        set_del = policy[ category ]["del"]

        curr_repo = "null"
        if (category == "nx"):
            curr_repo = nx_repo_alias
        elif (category == "s3"):
            curr_repo = s3_repo_alias

        print("Delta ({}): {} -----".format( category, curr_repo ))

        if ((len(set_add) + len(set_del)) == 0):
            print("* No Change *")
            continue

        if (len(set_add) > 0):
            report_object_list( list(set_add), "", "+" )
        if (len(set_del) > 0):
            report_object_list( list(set_del), "", "-" )

    return

def report_object_list(object_list, header="", pips=">"):
    object_list.sort()

    if len(header) > 0:
        print("===== {header} =====".format( header=header ))

    for obj in object_list:
        print("{pips} {obj}".format(pips=pips, obj=obj))

    return

def perform_sweep(nx_repo_alias, s3_repo_alias, file_deletion_threshold=300, detailed_report=True):
    # Initialize nx_objects
    nx_objects = []
    s3_objects = []

    policy = {
        "nx": {
            "del": set(),
            "add": set()
        },
        "s3": {
            "del": set(),
            "add": set()
        }
    }

    # Get the facts (list of Nexus and AwsS3 objects) 
    # nx_objects = list_nexus( "bridge_raw" )
    nx_objects = set( list_nexus( nx_repo_alias ) )
    s3_objects = set( list_awss3( s3_repo_alias ) )

    # Report the object list (RAW)
    if detailed_report:
        report_object_list(list(nx_objects), "Full List (NX): {}".format( nx_repo_alias ))
        report_object_list(list(s3_objects), "Full List (S3): {}".format( s3_repo_alias ))

    # Get the dictionary of locked objects
    dict_nx_locked_objects = get_filetype_by_regex( nx_objects, "lock", True )
    dict_s3_locked_objects = get_filetype_by_regex( s3_objects, "lock", True )
    # Compile a set of locked objects
    set_nx_locked_objects = ( set(dict_nx_locked_objects.keys()) | set(dict_nx_locked_objects.values()) )
    set_s3_locked_objects = ( set(dict_s3_locked_objects.keys()) | set(dict_s3_locked_objects.values()) )

    # Exclude locked objects from nx and s3 
    nx_objects = nx_objects - set_nx_locked_objects
    s3_objects = s3_objects - set_s3_locked_objects

    # Report the object list
    # report_object_list(list(nx_objects), "Trimmed List (NX): {}".format( nx_repo_alias ))
    # report_object_list(list(s3_objects), "Trimmed List (S3): {}".format( s3_repo_alias ))

    # Set the base policy   (Nexus <-add/del-- S3)
    policy = sweep_set_standard_policy(policy, nx_objects, s3_objects)
    # Set the push policy   (Nexus --add/xxx-> S3)
    policy = sweep_apply_push_policy(policy, policy["nx"]["del"], nx_repo_alias, file_deletion_threshold )
    # Set the delete policy (Nexus --xxx/del-> S3)
    policy = sweep_apply_delete_policy(policy, nx_objects, set_nx_locked_objects | set_s3_locked_objects )
    # Set clamping of the policy
    policy = sweep_apply_clamp_policy(policy, nx_objects, s3_objects)

    # Provide the policy deltas
    report_policy( policy, nx_repo_alias, s3_repo_alias )

    # Apply the policy (First ADD, then DELETE)
    apply_policy( policy, nx_repo_alias, s3_repo_alias )

    return

def main():
    bridge_appdir_tmp="{bridge_appdir}/tmp".format( bridge_appdir=BRIDGE_APPDIR )
    try:
        os.mkdir( bridge_appdir_tmp )
        print("tmp/ folder {} created.".format( bridge_appdir_tmp ))
    except FileExistsError:
        pass

    if not os.path.exists(BRIDGE_FILE_SHUTDOWN):
        os.mknod(BRIDGE_FILE_SHUTDOWN)
        time.sleep(1)

    if not os.path.exists(BRIDGE_FILE_FORCE_SWEEP):
        os.mknod(BRIDGE_FILE_FORCE_SWEEP)
        time.sleep(1)

    file_deletion_threshold = NUMBER_OF_SECONDS_PER_SWEEP + 1

    sweep_counter = 0 
    while(True):
        print("Bridge @ {}".format( datetime.datetime.now() ))

        authorized_to_execute = True

        shutdown_file_age = datetime.datetime.now().timestamp() - os.path.getmtime(BRIDGE_FILE_SHUTDOWN)
        if (shutdown_file_age < 1.05):
            print("Bridge exitted at {}".format( datetime.datetime.now() ))
            exit(0)

        sweep_file_age = datetime.datetime.now().timestamp() - os.path.getmtime(BRIDGE_FILE_FORCE_SWEEP)
        if (sweep_file_age >= 1.05):
            authorized_to_execute = False

        if (authorized_to_execute) or (sweep_counter <= 0):
            print("Execute!")
            
            nx_raw_repo_alias = "bridge_raw"
            s3_raw_repo_alias = "raw"
            perform_sweep( nx_raw_repo_alias, s3_raw_repo_alias, file_deletion_threshold )

            nx_mvn_repo_alias = "bridge_mvn"
            s3_mvn_repo_alias = "mvn"
            perform_sweep( nx_mvn_repo_alias, s3_mvn_repo_alias, file_deletion_threshold )

            sweep_counter = NUMBER_OF_SECONDS_PER_SWEEP
        
        sweep_counter -= 1
        time.sleep(1)
        # break
    return

if __name__ == "__main__":
    main()


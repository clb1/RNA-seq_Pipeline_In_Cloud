#!/usr/bin/env python

# For handling preemption events:
# https://groups.google.com/forum/#!topic/gce-discussion/Ox2KV9CjVx0
#

import sys
import time
from collections import defaultdict, namedtuple

from apiclient import discovery
from oauth2client.client import GoogleCredentials
from googleapiclient.errors import HttpError


ConfigData = namedtuple("ConfigData",
                        "project_name, bucket_name, zone_names, disk_image, boot_disk_size_gb, base_machine_type, \
                        num_allowed_instances_per_zone, num_tasks_per_instance, time_limit, task_description, \
                        src_directory, dest_directory")


def initializeServices():
    credentials = GoogleCredentials.get_application_default()
    storage_service = discovery.build('storage', 'v1', credentials=credentials)
    compute_service = discovery.build('compute', 'v1', credentials=credentials)
    return (storage_service, compute_service)


def readConfigFile(pipeline_stage, config_file):
    config_data = {}
    section_header = "[%s]" % pipeline_stage
    read_config = False

    # Read relevant portion of config file into a dictionary,
    # then convert dictionary to a namedtuple
    ip = open(config_file, 'r')
    for line in ip:
        line = line.strip()
        if (line.startswith('[')):
            read_config = line.startswith(section_header)
        elif (read_config and len(line) > 0 and line[0] != '#'):
            if ('#' in line):
                line = line.split('#')[0]
            line = line.strip()
            variable, value = line.split('=')
            variable = variable.strip()
            value = value.strip()

            if (value.isdigit()):
                value = int(value)
            elif (',' in value):
                value = value.split(',')
                
            config_data[variable] = value
            
    ip.close()

    return ConfigData(**config_data)


def createInstance(compute_service, project_name, zone_name, instance_name, tasks, task_description, CD, max_tries=30):
    startup_script = "make -C /home/cbarrett/run &> make.err" #open('startup-script.sh', 'r').read()
    machine_type = "zones/%s/machineTypes/%s" % (zone_name, CD.base_machine_type)
    tasks_string = "+".join(tasks)
    
    config = {
        'name': instance_name,
        'machineType': machine_type,
        'description': task_description,

        # Specify the boot disk and the image to use as a source.
        'disks': [
            {
                'boot': True,
                'autoDelete': True,
                'initializeParams': {
                    'diskSizeGb': str(CD.boot_disk_size_gb),
                    'sourceImage': CD.disk_image,
                }
            }
        ],

        # Specify a network interface with NAT to access the public
        # internet.
        'networkInterfaces': [{
            'network': 'global/networks/default',
            'accessConfigs': [
                {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
            ]
        }],

        # Allow the instance to access cloud storage and logging.
        'serviceAccounts': [{
            'email': 'default',
            'scopes': [
                'https://www.googleapis.com/auth/devstorage.read_write',
                'https://www.googleapis.com/auth/logging.write'
            ]
        }],

        # Metadata is readable from the instance and allows you to
        # pass configuration from deployment scripts to instances.
        'metadata': {
            'items': [{
                # Startup script is automatically executed by the
                # instance upon startup.
                'key': 'startup-script',
                'value': startup_script
            }, {
                'key': 'tasks',
                'value': tasks_string
            }, {
                'key': 'timelimit',
                'value': CD.time_limit
            }, {
                'key': 'target_suffix',
                'value': 'R1.fa.gz'
            }, {
                # Every project has a default Cloud Storage bucket that's
                # the same name as the project.
                'key': 'bucket',
                'value': project_name # TODO: Evaluate whether needed
            }]
        }
    }

    operation = compute_service.instances().insert(project=project_name, zone=zone_name, body=config).execute()

    not_done = True
    instance_was_created = True
    err_code = ""
    try_count = 0
    while (not_done and try_count <= max_tries):
        try_count += 1
        time.sleep(1)
        try:
            op_result = compute_service.zoneOperations().get(project=project_name, zone=zone_name, operation=operation["name"]).execute()
        except HttpError, http_err:
            print http_err
            
        if (op_result['status'] == "DONE"):
            not_done = False
            if (op_result.has_key("error")):
                instance_was_created = False
                for err in op_result["error"]["errors"]:
                    err_code = err["code"]
                    print >> sys.stderr, "ERROR in createInstance(): %s" % err["message"]

    if (not_done and err_code==""):
        err_code = "MAX TRIES EXCEEDED"

    return (instance_was_created, err_code)


# See https://cloud.google.com/compute/docs/tutorials/python-guide#stoppinganinstance
def deleteInstance(compute_service, project_name, zone_name, instance_name):
    operation = compute_service.instances().delete(project=project_name, zone=zone_name, instance=instance_name).execute()

    not_done = True
    instance_was_deleted = True
    while (not_done):
        time.sleep(1)
        op_result = compute_service.zoneOperations().get(project=project_name, zone=zone_name, operation=operation["name"]).execute()
        if (op_result['status'] == "DONE"):
            not_done = False
            if (op_result.has_key("error")):
                instance_was_deleted = False
                for err in op_result["error"]["errors"]:
                    print >> sys.stderr, "ERROR in deleteInstance(): %s" % err["message"]

    return instance_was_deleted


def stopInstance(compute_service, project_name, zone_name, instance_name):
    operation = compute_service.instances().stop(project=project_name, zone=zone_name, instance=instance_name).execute()

    not_done = True
    instance_was_stopped = True
    while (not_done):
        time.sleep(1)
        op_result = compute_service.zoneOperations().get(project=project_name, zone=zone_name, operation=operation["name"]).execute()
        if (op_result['status'] == "DONE"):
            not_done = False
            if (op_result.has_key("error")):
                instance_was_stopped = False
                for err in op_result["error"]["errors"]:
                    print >> sys.stderr, "ERROR in stopInstance(): %s" % err["message"]

    return instance_was_stopped


def queryInstanceStates(compute_service, project_name, zone_name, task_description):
    deployed_instances = set()

    request = compute_service.instances().list(project=project_name, zone=zone_name, maxResults=500)
    listing = request.execute()

    if (listing.has_key("items")):
        # From instances in listing["items"]
        # ["name"] is for refering to this instance, created by me upon instance creation
        # ["status"] is one of PROVISIONING, STAGING, RUNNING, STOPPING, and TERMINATED
        # ["description"] is set by me upon instance creation. Only interested in "trim-fastq" instances
        # ["metadata"]["items"]["target fastqs"] is a list of fastq IDs set by me upon instance creation.
        for instance in listing["items"]:
            if (instance["description"] == task_description):
                deployed_instances.add( (instance["name"], instance["status"]) )
                
    return deployed_instances


def getReadyTasks(storage_service, CD, max_results=200):
    """Returns a set of <tissue>/<sample_ID> for files in the bucket subdirectory 'src_directory'."""

    # Not needed for all tasks
    mates_count = defaultdict(int)

    request = storage_service.objects().list(bucket=CD.bucket_name, prefix=CD.src_directory, maxResults=max_results, fields="nextPageToken,items(name)")
    while (request != None):
        listing = request.execute()

        if (CD.task_description == "trim-fastq"):
            # Expected format of uploaded fastq files: raw_fastq/TissueX/<sample ID>_R1.fastq.gz
            for (prefix_dir, tissue, filename) in map(lambda x: x["name"].split('/'), listing["items"]):
                index = "%s/%s" % (tissue, filename[0:-12])
                mates_count[index] += 1
        else:
            print >> sys.stderr, "ERROR: getReadyTasks(): unrecognized task description -> %s. Exiting." % CD.task_description
            sys.exit(1)
            
        if (listing.has_key("nextPageToken")):
            request = storage_service.objects().list(bucket=CD.bucket_name, prefix=CD.src_directory, maxResults=max_results,
                                                     fields="nextPageToken,items(name)", pageToken=listing["nextPageToken"])
        else:
            request = None
        
    # Transformed listing results into set of tasks ready to be processed by pipeline stage
    if (CD.task_description == "trim-fastq"):
        # Get the prefix for which the fastq of both mates (R1 & R2) exist
        ready_for_pipeline = set(map(lambda y:y[0], filter(lambda x:x[1]==2, mates_count.items())))

    return ready_for_pipeline


def checkForTaskCompletion(storage_service, target_tasks, CD):
    defunct_uncompleted_tasks = set()

    for target_task in target_tasks:
        files_prefix = "%s/%s" % (CD.dest_directory, target_task)
        request = storage_service.objects().list(bucket=CD.bucket_name, prefix=files_prefix, maxResults=10, fields="nextPageToken,items(name)")
        listing = request.execute()
        if (listing.has_key("items")):
            files = map(lambda x: x["name"], listing["items"])
        else:
            files = []

        if (CD.task_description == "trim-fastq"):
            assert (len(files) <= 2)
            # Check both files (for the read pair) present and complete
            file_R1 = "%s/%s_R1.fa.gz" % (CD.dest_directory, target_task)
            file_R2 = "%s/%s_R2.fa.gz" % (CD.dest_directory, target_task)
            if ( not(file_R1 in files and file_R2 in files) ):
                defunct_uncompleted_tasks.add( target_task )
        else:
            print >> sys.stderr, "ERROR: checkForTaskCompletion(): unrecognized task description -> %s. Exiting." % CD.task_description
            sys.exit(1)
            
    return defunct_uncompleted_tasks


def partitionTasks(unassigned_tasks, num_new_to_start, num_tasks_per_instance):
    """Returns list of lists of <tissue>/<sample ID>, where the secondary lists are of length <= num_tasks_per_instance"""

    partitions = []
    for i in xrange(num_new_to_start):
        partitions.append([])

    # Distribute tasks evenly across all jobs
    for j, task in enumerate(unassigned_tasks):
        index = j % num_new_to_start
        partitions[index].append(task)

    # Truncate tasks assigned to the maximum allowed per instance
    for k in xrange(num_new_to_start):
        partitions[k] = partitions[k][0:num_tasks_per_instance]

    return partitions


def getInstanceFromPool(zone_labeled_instances, currently_problematic_zones):
    zone_name, instance_name = (None, None)

    for candidate_zone in zone_labeled_instances.keys():
        if (candidate_zone not in currently_problematic_zones and len(zone_labeled_instances[candidate_zone]) > 0):
            zone_name = candidate_zone
            instance_name = zone_labeled_instances[candidate_zone].pop()
            break

    return (zone_name, instance_name)


def deployInstances(storage_service, compute_service, zone_labeled_instances, all_deployed_instances, assigned_tasks, instance_assigned_tasks, CD):
    unassigned_tasks = getReadyTasks(storage_service, CD)
    unassigned_tasks -= assigned_tasks
    
    max_num_allowed_instances = len(zone_labeled_instances.keys()) * CD.num_allowed_instances_per_zone
    
    project_name = CD.project_name
    num_tasks_per_instance = CD.num_tasks_per_instance
    task_description = CD.task_description
    
    while (len(unassigned_tasks) > 0 or len(all_deployed_instances) > 0):
        # Determine whether terminated instances completed their tasks
        for zone_name in zone_labeled_instances.keys():
            deployed_instances = queryInstanceStates(compute_service, project_name, zone_name, task_description)
            for (instance_name, instance_status) in deployed_instances:
                if (instance_status in ["STOPPING", "TERMINATED"]): # VM has been preempted by GCE or exceeded uptime limit and stopped
                    # Determine which tasks were and were not completed for instances that are no longer active
                    defunct_uncompleted_tasks = checkForTaskCompletion(storage_service, instance_assigned_tasks[(zone_name, instance_name)], CD)
                    assigned_tasks -= defunct_uncompleted_tasks
                    unassigned_tasks.update(defunct_uncompleted_tasks)

                    # Recycle the instance
                    instance_was_deleted = deleteInstance(compute_service, project_name, zone_name, instance_name)
                    assert(instance_was_deleted)
                    del instance_assigned_tasks[(zone_name,instance_name)]
                    all_deployed_instances.remove( (zone_name, instance_name) )
                    zone_labeled_instances[zone_name].append(instance_name)
                    
        # Start new instances
        unassigned_tasks -= assigned_tasks
        num_new_to_start = min(len(unassigned_tasks), max_num_allowed_instances - len(all_deployed_instances))
        partitions = partitionTasks(unassigned_tasks, num_new_to_start, num_tasks_per_instance)

        # This section needs to be resilient to inability to start new instances because of resource exhaustion
        currently_problematic_zones = set()
        for a_task_set in partitions:
            zone_name, instance_name = getInstanceFromPool(zone_labeled_instances, currently_problematic_zones)
            if ( (zone_name, instance_name) != (None, None) ):
                instance_was_created, err_code = createInstance(compute_service, project_name, zone_name, instance_name, a_task_set, task_description, CD)
                if (instance_was_created):
                    print >> sys.stderr, "STARTED %s in zone %s for %s" % (instance_name, zone_name, ", ".join(a_task_set))
                    instance_assigned_tasks[(zone_name, instance_name)] = a_task_set
                    assigned_tasks.update(a_task_set)
                    all_deployed_instances.add( (zone_name, instance_name) )
                elif (err_code == "MAX TRIES EXCEEDED"):
                    currently_problematic_zones.add(zone_name)
            else:
                break
            
        # Throttle rate of directory listing queries
        time.sleep(30)
        
        # Update
        all_tasks = getReadyTasks(storage_service, CD)
        unassigned_tasks = all_tasks - assigned_tasks


def labelInstancesPerZone(compute_service, CD):
    zone_labeled_instances = {}
    counter = 1
    for zone_name in CD.zone_names:
        zone_labeled_instances[zone_name] = map(lambda x: "%s-%d" % (CD.task_description, x), range(counter,counter+CD.num_allowed_instances_per_zone))
        counter += CD.num_allowed_instances_per_zone
    return zone_labeled_instances


def initializeBasedOnDeployedInstances(compute_service, CD):
    all_deployed_instances = set()
    assigned_tasks = set()
    instance_assigned_tasks = {}

    expected_zones = zone_labeled_instances.keys()

    project_name = CD.project_name
    task_description = CD.task_description
    
    # Get all zones in US region 
    request = compute_service.zones().list(project=project_name, maxResults=100)
    listing = request.execute()
    us_zones = map(lambda y: y["name"], filter(lambda x: x["name"].startswith("us"), listing["items"]))

    # Get all relevant instances existing (in any status state) in the zones
    found_deployed_instances = []
    for us_zone in us_zones:
        list_of_name_and_status = queryInstanceStates(compute_service, project_name, us_zone, task_description)
        for instance_name, instance_status in list_of_name_and_status:
            found_deployed_instances.append( (us_zone, instance_name, instance_status) )

    for (zone_name, instance_name, instance_status) in found_deployed_instances:
        print >> sys.stderr, instance_name
        all_deployed_instances.add( (zone_name, instance_name) )
        request = compute_service.instances().get(project=project_name, zone=zone_name, instance=instance_name)
        instance = request.execute()
        if (instance["description"] == task_description):
            tasks_part = filter(lambda x: x["key"]=="tasks", instance["metadata"]["items"])
            assert(len(tasks_part) == 1)
            tasks = set(tasks_part[0]["value"].split('+'))
            assigned_tasks.update(tasks)
            instance_assigned_tasks[(zone_name,instance_name)] = tasks
            
            # Remove instance from list of instances that can be deployed in the zone
            if (zone_name not in expected_zones):
                print >> sys.stderr, "ERROR: zone %s is not one of the expected zones. Exiting." % zone_name
                sys.exit(1)
            else:
                assert (instance_name in zone_labeled_instances[zone_name])
                zone_labeled_instances[zone_name].remove(instance_name)
                
    return (all_deployed_instances, assigned_tasks, instance_assigned_tasks)


def testStartInstanceWithMetadata(compute_service, project_name, zone_name, disk_image, boot_disk_size, base_machine_type):
    tasks = ["TissueX/TCGA-AB-2803-03A-01T-0734-13", "TissueY/TCGA-AB-2808-03A-01T-0734-13"]
    
    print >> sys.stderr, "INFO: starting instance"
    instance_was_created = createInstance(compute_service, project_name, zone_name, "metadata-test", disk_image, boot_disk_size, base_machine_type, tasks)

    print >> sys.stderr, "INFO: stopping instance"
    instance_was_stopped = stopInstance(compute_service, project_name, zone_name, "metadata-test")

    request = compute_service.instances().list(project=project_name, zone=zone_name)
    listing = request.execute()

    print >> sys.stderr, "INFO: deleting instance"
    instance_was_deleted = deleteInstance(compute_service, project_name, zone_name, "metadata-test")

    request = compute_service.instances().list(project=project_name, zone=zone_name)
    listing = request.execute()

    print >> sys.stderr, "INFO: testing completed"


if (__name__ == "__main__"):
    pipeline_stage_name, config_file = sys.argv[1:]
    
    storage_service, compute_service = initializeServices()
    CD = readConfigFile(pipeline_stage_name, config_file)
    zone_labeled_instances = labelInstancesPerZone(compute_service, CD)

    all_deployed_instances, assigned_tasks, instance_assigned_tasks = initializeBasedOnDeployedInstances(compute_service, CD)

    #testStartInstanceWithMetadata(compute_service, project_name, zone_names[0], disk_image, boot_disk_size_gb, base_machine_type)
    deployInstances(storage_service, compute_service, zone_labeled_instances, all_deployed_instances, assigned_tasks, instance_assigned_tasks, CD)

    sys.exit(0)

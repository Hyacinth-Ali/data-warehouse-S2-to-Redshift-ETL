import asyncio
from time import sleep
import pandas as pd
import boto3
import json
import psycopg2
import configparser

from provision_resource_helper import create_aws_client, create_aws_resource, create_iam_role

# Datawarehouse parameters
# Access Keys
KEY                     = ''
SECRET                  = ''

# Cluster Details
DWH_CLUSTER_TYPE        = ''
DWH_NUM_NODES           = ''
DWH_NODE_TYPE           = ''
DWH_CLUSTER_IDENTIFIER  = ''
DWH_IAM_ROLE_NAME       = ''

# Cluster Database Details
HOST                    = ''
DB_NAME                 = ''
DB_USER                 = ''
DB_PASSWORD             = ''
DB_PORT                 = ''

ec2                     = ''
s3                      = ''
iam                     = ''
redshift                = ''
roleArn                 = ''

async def init_datawarehouse_params():
    """
    Loads datawarehouse credentials that are required to
    provision the computing resource in AWS Redshift datawarehouse.
    Also, the credentials allows the project to interact with
    the databases in the Redshift clusters.

    open the configuration file so that the credntials values
    can be extracted to initialize the corresponding variables
    """
        
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    print("Loading datawarehouse parameter values")
    # Access Keys
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    # Cluster Details
    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

    # Cluster Database Details
    HOST                   = config.get("CLUSTER", "HOST")
    DB_NAME                = config.get("CLUSTER","DB_NAME")
    DB_USER                = config.get("CLUSTER","DB_USER")
    DB_PASSWORD            = config.get("CLUSTER","DB_PASSWORD")
    DB_PORT                = config.get("CLUSTER","DB_PORT")

    print("Done Loading datawarehouse parameter values")

    print("Creating ec2, s3, iam, and reshift clients")
    # Create ec2 and s3 aws resources
    ec2 = create_aws_resource('ec2', 'us-west-2', KEY, SECRET)
    s3 = create_aws_resource('s3', 'us-west-2', KEY, SECRET)

    #Create iam andredshift aws clients
    iam = create_aws_client('iam', 'us-west-2', KEY, SECRET)
    redshift = create_aws_client('redshift', 'us-west-2', KEY, SECRET)
    print("Done creating ec2, s3, iam, and reshift clients")

    # create iam role
    print("Creating IAM role")
    roleArn = create_iam_role(iam, DWH_IAM_ROLE_NAME)
    print("Done creating IAM role")

async def create_redshift_cluster():
    """
    Creates a redshift cluster that will be used as the cloud dataware to host
    the databses that powers the data analysis
    """
    try:
        response = redshift.create_cluster(        
            #Hardware
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DB_NAME,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
        
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print("Error: Issues creating redshift cluster")
        print(e)

async def get_cluster_details():
    """
    Retrieves the cluster details after the cluster status becomes available
    """
    count = 1
    # describe redshift properties to if ClusterStatus is avaiable
    print('{}: check if the cluster is available'.format(count))
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    # Wait until the cluster status becomes available
    while myClusterProps['ClusterStatus'] != 'available':
        sleep(3)
        count += 1
        print('{}: check if the cluster is available'.format(count))
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        
    # Take note of the cluster endpoint and role ARN
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

    # Open an incoming TCP port to access the cluster ednpoint
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT)
        )
    except Exception as e:
        print("Error: Issues opnening TCP port")
        print(e)

    # Validate connection to the redshift cluster
    try: 
        connect = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT))
    except psycopg2.Error as e: 
        print("Error: Could not make connection to the cluster database")
        print(e)
    try: 
        cursor = connect.cursor()
    except psycopg2.Error as e: 
        print("Error: Could not get cursor to the Clsuter Database")
        print(e)
    connect.set_session(autocommit=True)
    print("Successfully connected to the redshift")

async def provosion_resources():
    """
    Schedules calls to provision the aws resources
    """
    print(f"started provisioning of datawarehouse resources")

    # Initialize datawarehouse parameter values
    await init_datawarehouse_params()

    # Create redshift cluster
    print("Creating redshift cluster")
    await create_redshift_cluster()
    print("After called to creating redshift cluster")

    # Get redshift cluster
    print("get the cluster details")
    await get_cluster_details()
    print("Done getting the cluster details")

asyncio.run(provosion_resources())

from time import sleep
import pandas as pd
import boto3
import json
import psycopg2
import configparser


# Loads datawarehouse credentials that are required to
# provision the computing resource in AWS Redshift datawarehouse.
# Also, the credentials allows the project to interact with
# the databases in the Redshift clusters.

# open the configuration file so that the credntials values
# can be extracted to initialize the corresponding variables
    
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

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

def create_aws_resource(name, region):
    """
    Creates an AWS resource, e.g., ec2, s3.
    
    :param str name - the name of the resource
    :param str region - the name of the AWS region that will contain the resource.

    :return - the AWS resource

    :raises UnknownServiceError - if the name is an invalid AWS resource name
    """
    try:
        resource = boto3.resource(
            name,
            region_name=region,
            aws_access_key_id=KEY,
            aws_secret_access_key=SECRET
        )
    except Exception as e:
        print("Error: Issues creating the aws resource")
        raise(e)
    return resource

def create_aws_client(name, region):
    """
    Creates an AWS client, e.g., ima, redshift.
    
    :param str name - the name of the client
    :param str region - the name of the AWS region that will contain the client.

    :return - the AWS client
    """

    try:
        client = boto3.client(
            name,
            region_name=region,
            aws_access_key_id=KEY,
            aws_secret_access_key=SECRET
        ) 
    except Exception as e:
        print("Error: Issues creating the aws client")
        raise(e)

    return client

# Create different aws resources and clients
ec2 = create_aws_resource('ec2', 'us-west-2')
s3 = create_aws_resource('s3', 'us-west-2')
iam = create_aws_client('iam', 'us-west-2')
redshift = create_aws_client('redshift', 'us-west-2')

def create_iam_role():
    """
    Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)
    """
    #1.1 Create the role, 
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print("Error: Issues creating iam role")
        print(e)        
    
    
    print("1.2 Attaching Policy")
    iam.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)

    return roleArn

# create iam role
roleArn = create_iam_role()

def create_redshift_cluster():
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

def getClusterDetails():
    """
    Retrieves the cluster details after the cluster status becomes available
    """
    # describe redshift properties to if ClusterStatus is avaiable
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    # Wait until the cluster status becomes available
    while myClusterProps['ClusterStatus'] != 'available':
        sleep(3)
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
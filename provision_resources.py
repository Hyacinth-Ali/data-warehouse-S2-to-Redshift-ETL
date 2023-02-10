from time import sleep
import pandas as pd
import psycopg2
import configparser

from provision_resource_helper import create_aws_client, create_aws_resource, create_iam_role

"""
Loads datawarehouse credentials that are required to
provision the computing resource in AWS Redshift datawarehouse.
Also, the credentials allows the project to interact with
the databases in the Redshift clusters.
"""
            
# open the configuration file so that the credntials values
# can be extracted to initialize the corresponding variables
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

iam_role_arn = create_iam_role(iam, DWH_IAM_ROLE_NAME)

 
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
            IamRoles=[iam_role_arn]  
        )
    except Exception as e:
        print("Error: Issues creating redshift cluster")
        print(e)
        raise(e)

if __name__ == "__main__":
    # Create redshift cluster
    print("Creating redshift cluster")
    create_redshift_cluster()
    print("After called to create redshift cluster")
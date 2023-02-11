import configparser
from time import sleep
import psycopg2
from provision_resource_helper import create_aws_client, create_aws_resource
from sql_queries import create_table_queries, drop_table_queries

config = configparser.ConfigParser()

# Read configuration values
config.read('dwh.cfg')

# Access Keys
KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

# Cluster Details
DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
DWH_ENDPOINT           = config.get("DWH", "DWH_ENDPOINT")

# Cluster Database Details
HOST                   = config.get("CLUSTER", "HOST")
DB_NAME                = config.get("CLUSTER","DB_NAME")
DB_USER                = config.get("CLUSTER","DB_USER")
DB_PASSWORD            = config.get("CLUSTER","DB_PASSWORD")
DB_PORT                = config.get("CLUSTER","DB_PORT")

print("Creating ec2, s3, iam, and reshift clients")
# Create ec2 and s3 aws resources
ec2 = create_aws_resource('ec2', 'us-west-2', KEY, SECRET)
s3 = create_aws_resource('s3', 'us-west-2', KEY, SECRET)
        
#Create iam andredshift aws clients
iam = create_aws_client('iam', 'us-west-2', KEY, SECRET)
redshift = create_aws_client('redshift', 'us-west-2', KEY, SECRET)
print("Done creating ec2, s3, iam, and reshift clients")


def drop_tables(cur, conn):
    """
    Drop the database tables that already exist.
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            print("Dropped a table")
            conn.commit()
        except Exception as e:
            print("Error: The table doesn't exist")

def create_tables(cur, conn):
    for query in create_table_queries:
        try:
            cur.execute(query)
            print("Created a table")
            conn.commit()
        except Exception as e:
            print("Error: Issues creating a table")
            print(e)

def validate_redshift():
    """
    Retrieves the cluster details after the cluster status becomes available

    Returns - redshift that can be used to query redshift databases
    """
    count = 1
    # describe redshift properties to if ClusterStatus is avaiable
    print('{}: check if the cluster is available'.format(count))
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    # Wait until the cluster status becomes available
    while myClusterProps['ClusterStatus'] != 'available':
        sleep(15)
        count += 1
        print('{}: check if the cluster is available'.format(count))
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    
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
        print("Error: TCP port already exist")
        # print(e)

    # Validate connection to the redshift cluster
    try: 
        global connect
        connect = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT))
    except psycopg2.Error as e: 
        print("Error: Could not make connection to the cluster database")
        print(e)
    try: 
        global cursor
        cursor = connect.cursor()
    except psycopg2.Error as e: 
        print("Error: Could not get cursor to the Clsuter Database")
        print(e)
    connect.set_session(autocommit=True)
    print("Successfully connected to the redshift")
    return connect



def main():

    # wait for initialization to complete
    # sleep(2)

    # Check if redshift cluster is available and then connect to the redshift
    validate_redshift()

    # For a flexible implementation as well as experiments, drop tables and then create tables
    # This approach ensures that we always reset the tables to test the ETL pipeline
    drop_tables(cursor, connect)
    create_tables(cursor, connect)

    connect.close()


if __name__ == "__main__":
    main()
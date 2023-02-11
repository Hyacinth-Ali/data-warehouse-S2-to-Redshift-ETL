import configparser
from time import sleep
from provision_resource_helper import create_aws_client

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
KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME = config.get("DWH","DWH_IAM_ROLE_NAME")
redshift = create_aws_client('redshift', 'us-west-2', KEY, SECRET)
#Create iam andredshift aws clients
iam = create_aws_client('iam', 'us-west-2', KEY, SECRET)
        

def delete_cluster():
    """
    Deletes an existing cluster 

    Args:
        redshift (_type_): the cluster to be deleted
        DWH_CLUSTER_IDENTIFIER (_type_): _the cluster identifier type_
    """
    # Delete redshift cluster
    isSuccess = False
    try:
        print("Delete redshift cluster")
        redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
        isSuccess = True
        print("After called to delete redshift cluster")
    except Exception as e:
        print("Error: Issues deleting the cluster")
        print(e)

    count = 1
    # describe redshift properties to if ClusterStatus is not avaiable
    # Stop the iteration if the counter gets up to 10 or the cluster is deleted
    while count <= 100:
        try:
            # This call will throw an exception after the cluster is fully deleted
            print('{}: check if the cluster is still available'.format(count))
            redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            sleep(20)
            count += 1
        except Exception as e:
            if (isSuccess):
                print("Successfully deleted the cluster")
            break
    
def delete_role_arn():
    """
    Deletes role name arn

    Args:
        iam (Any): the iam client
        DWH_IAM_ROLE_NAME (Any): the datawarehouse iam role name
    """
    try:
        iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
        print("Deleted the role arn")
    except Exception as e:
        print("Error: Issues deleting the role arn")
        print(e)


if __name__ == "__main__":
    delete_cluster()
    delete_role_arn()



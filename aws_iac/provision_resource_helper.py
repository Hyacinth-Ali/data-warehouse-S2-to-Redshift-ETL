import boto3
import json

def create_aws_resource(name, region, KEY, SECRET):
    """
    Creates an AWS resource, e.g., ec2, s3.
    
    :param str name - the name of the resource
    :param str region - the name of the AWS region that will contain the resource.
    :param str KEY - the aws access key
    :param str SECRET - the aws access secret key
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

def create_aws_client(name, region, KEY, SECRET):
    """
    Creates an AWS client, e.g., ima, redshift.
    
    :param str name - the name of the client
    :param str region - the name of the AWS region that will contain the client.
    :param str KEY - the aws access key
    :param str SECRET - the aws access secret key

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

def create_iam_role(iam, DWH_IAM_ROLE_NAME):
    """
    Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)
    
    :param iam Any: the iam client
    :param DWH_IAM_ROLE_NAME Any: the name for the iam role

    :return - the aws iam role
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

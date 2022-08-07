# AWS Code
This file contains instructions + code for interacting with AWS.

## Setting up IAM Role for Redshift
As part of the Udacity course, I was instructed to create an IAM role for **Redshift** to allow it **Read only access to S3 buckets**

From my experience, I know that creating roles are a way to manage how AWS services can interact with each other (e.g., can a Lambda function pull data from the Simple Queue Service?)

>I was looking for a way to create and edit this role programmatically rather than using the web UI, but I didn't find it.
Even after I created the role, I didn't find a complete JSON or other computer-level description of the role.
The only thing I was able to find was a JSON-level description of the permissions I granted Redshift:

**AmazonS3ReadOnlyAccess**
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:Get*",
                "s3:List*",
                "s3-object-lambda:Get*",
                "s3-object-lambda:List*"
            ],
            "Resource": "*"
        }
    ]
}
```

## Creating an AWS Security Group
>Udacity course: *"A security group will act as firewall rules for your Redshift cluster to control inbound and outbound traffic."*

1. Navigate to **EC2** service
2. Navigation & Security menu -> Security Groups
3. "Create Security Group" button
4. Name and create **default Virtual Private Cloud (VPC)**
   1. Note: What would it mean to create a custom one? When might I want to do this?
5. Make inbound rule
   1. Custom TCP rule
   2. Protocol = TCP
   3. Port 5439 (default port for Redshift)
   4. Source Type = Custom
   5. Source = `0.0.0.0/0`
      1. **Important: this all-zeroes IP address allows ALL incoming connectcions**. It's actually terrible for security.
         1. How to specify a better IP?
         2. **Note: I am going to specify my own IP address and see if I can make it work like that instead.**


## Create a Redshift Cluster Instance
>**NOTE:** You are charged for any running redshift instances! **Make sure you DELETE all Redshift clusters before I log off EACH TIME (need to re-create cluster upon re-starting session).**

1. Navigate to Redshift service
2. Select "Create Cluster"
3. Choose "Free Trial" option
4. Configure database for `port=5439` (only important it matches your security preferences)
5. set username & password
6. Set cluster permissions to the IAM role which allowed Redshift access to S3
7. In 'additional configurations', choose:
   1. Default VPC (unless you made a custom one)
   2. the VPC security group you created above
   3. Set availability zone
   4. Set VPC Routing (not sure what this is yet)
   5. Set whether database is publicly accessible
   
## Create S3 Bucket
1. Navigate to S3 service
2. Select "Create Bucket"
3. Set name of bucket
4. Set region of bucket
5. De-select "Block all public access"
   1. Note: In real life when I might want *some* public access, how can I configure this better?
6. Select if you want bucket versioning
7. Select if you want bucket encryption
8. Open advanced settings
   1. Select if you want object lock

## Create Postgres Database
1. Navigate to RDS (Relational Database Service)
2. Select "Create Database"
3. Select "Standard Create"
4. Select Engine (Postgres)
5. Select Template (Free Tier)
6. Give name for DB instance
7. set Username and password
8. Set configuration of instance
   1. What does "DB Instance Class" mean? i.e., DB server?
9. Choose storage system (e.g., use SSD drives or something else)
10. Choose size of storage (min. 20GiB)
11. Choose auto-scaling of storage (how much can DB grow?)
12. Connectivity
    1.  Choose default VPC (Again, when would I choose another??)
    2.  Allow public access (again, how could I make this more specific in the future?)
    3.  Choose port (5432 is Postgres default)
    4.  
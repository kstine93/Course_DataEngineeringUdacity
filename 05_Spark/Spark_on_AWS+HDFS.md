# Spark on AWS and HDFS
_Udacity - Data Engineering Nanodegree_

## Spark Cluster: Local vs. Standalone modes
In local mode, Spark runs on a single, local machine.
In standalone mode, I will need to use a resource manager like **YARN** or **Mesos** to coordinate the cluster. I will also need to "**define Spark Primary and Secondary**" (not sure what this is) and I will need to do this on AWS' Elastic MapReduce tool (EMR) or on my local machine.

---

#### Overview of a typical Spark Cluster on AWS
- We will store the data in **S3**
- We will run our spark cluster on a series of EC2 instances
  - Question: How to easily install same image on each instance?


---

## EC2 vs. EMR

**Summary:**
EMR is set up for big data processing - with faster connection to S3, native support for HDFS, and the tracking of nodes that (maybe) makes it more resilient to data loss in case of lost nodes (don't fully understand 'node categorization' yet).
Additionally, Spark is pre-installed on EMR machines, but not on EC2 machines.

|                                   |                                                         **AWS EMR**                                                        |                              **AWS EC2**                             |
|:---------------------------------:|:--------------------------------------------------------------------------------------------------------------------------:|:--------------------------------------------------------------------:|
| **Distributed computing**         | Yes                                                                                                                        | Yes                                                                  |
| **Node categorization**           | Categorizes secondary nodes into core and task nodes as a result of which data can be lost in case a data node is removed. | Does not use node categorization                                     |
| **Can support HDFS?**             | Yes                                                                                                                        | Only if you configure HDFS on EC2 yourself using multi-step process. |
| **What S3 protocol can be used?** | Uses S3 protocol over AWS S3, which is faster than s3a protocol                                                            | ECS uses s3a                                                         |
| **Comparison cost**               | Bit higher                                                                                                                 | Lower                                                                |

---

##### Review of AWS instance types
There are many different types of AWS instances which are optimized for different components:
- General purpose (ex: m5.large)
- Compute optimized (ex: c7g.large)
- Memory optimized (ex: r6a.large)
- Accelerated computing (ex: for graphics processing, p4d.24xlarge)
- Storage optimized (ex: Im4gn.large)

---

## Hadoop Distributed File System (HDFS) vs. S3
HDFS uses the MapReduce system as a resource manager to allow the distribution of files across the hard drives within a cluster (after jobs are completed, data is stored again).

---


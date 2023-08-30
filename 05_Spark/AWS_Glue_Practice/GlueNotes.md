# Notes on using AWS Glue

>**SIDE NOTE:** The AWS Cloudshell (accessible from the web interface) has built-in file storage. So, it's actually possible to put files on there and then move them around the AWS environment (e.g., move them to S3). This is probably best for testing purposes only, but still cool (e.g., the Udacity exercise had some data in GitHub. I could use `git clone` to move these files from Github into the Cloudshell file storage, and then use normal AWS CLI commands to move these now local files into an S3 bucket).

## Glue APIs
Glue APIs give access to resources like **Glue Tables** and **Glue Context** (TODO: Fill in what these actually do).

---

## Glue Studio
Glue Studio is a GUI for interacting with Glue to create Spark jobs with *added capabilites*.

Glue Studio also offers a drag-and-drop method of coding as well - along with some pre-defined common methods to apply to the data *and* preconfigured connections to common AWS data sources and sinks.

**Built-in transformation types:**
- Apply Mapping
- Select Fields
- Drop Fields
- Drop Null Fields
- Drop Duplicates
- Rename Field
- Spigot
- Join
- Split Fields
- Select from Collection
- Filter
- Union
- Aggregate
- Fill Missing Values
- **Custom Transform**
- **Custom SQL**
- Detect PII
- etc...

**Built-in data source/sinks:**
- S3
- Glue Table
- Dynamo
- Redshift
- MySQL
- PostgreSQL
- Microsoft SQL Server
- Oracle SQL
- etc...

### Personal Notes about Glue Studio
When should I use Glue Studio? It feels a bit weird to relinquish the details to the program. At the same time, I can't deny that having a visual representation of the data pipeline DAG is pretty nice for (a) understanding the pipeline and (b) actually editing it:

<img src="./media/glue_dag.jpeg" width = 60%>

I actually see no compelling reason **NOT** to use Glue for building my data pipelines:
- It has full integration with AWS resources
- It has built-in transformation code which is probably great for >90% of use cases (and I can build in performance tests to see if any of these are poorly optimized and build my own versions of the transformations if needed)
- It allows myself and others to see the DAG more clearly and debug/edit it than if we were using code alone
- Since I don't have to write the connections and transformations myself, it will probably speed up development a lot.

**Conclusion:**
Embrace the Glue.

---

## Tutorial: Setting up a pipeline on AWS Glue

### Setting up a Spark Job with Glue Studio
1. Open up AWS Glue Studio from the web interface
2. Select 'Jobs' from the side menu
3. Create a "Visual with a source and target" job. Specify that "S3" is the source and "S3" is also the target.
4. Name the job and select the appropriate role
   1. (see `./Glue_boto3Setup.ipynb` file in this directory for details on the role and policies used for this).


### Setting up a "filter" transformation


### Viewing logs & debugging


### Generate the Spark script
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1768988525845 = glueContext.create_dynamic_frame.from_catalog(database="stedi-udacitydb-vr", table_name="accelerometerdata", transformation_ctx="AWSGlueDataCatalog_node1768988525845")

job.commit()

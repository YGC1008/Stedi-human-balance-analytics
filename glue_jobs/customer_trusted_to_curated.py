import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node customer_trusted
customer_trusted_node1791300388078 = glueContext.create_dynamic_frame.from_catalog(database="yared", table_name="customer_trusted", transformation_ctx="customer_trusted_node1791300388078")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1791300389367 = glueContext.create_dynamic_frame.from_catalog(database="yared", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1791300389367")

# Script generated for node SQL Query
SqlQuery1600 = '''
SELECT DISTINCT c.*
FROM customer_trusted c
INNER JOIN accelerometer_trusted a
    ON c.email = a.user
'''
SQLQuery_node1791300459784 = sparkSqlQuery(glueContext, query = SqlQuery1600, mapping = {"accelerometer_trusted":accelerometer_trusted_node1791300389367, "customer_trusted":customer_trusted_node1791300388078}, transformation_ctx = "SQLQuery_node1791300459784")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1791300459784, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1791300220917", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1791300528997 = glueContext.getSink(path="s3://yared-gomez/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1791300528997")
AmazonS3_node1791300528997.setCatalogInfo(catalogDatabase="yared",catalogTableName="customer_curated")
AmazonS3_node1791300528997.setFormat("glueparquet", compression="snappy")
AmazonS3_node1791300528997.writeFrame(SQLQuery_node1791300459784)
job.commit()
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
customer_trusted_node1791299534692 = glueContext.create_dynamic_frame.from_catalog(database="yared", table_name="customer_trusted", transformation_ctx="customer_trusted_node1791299534692")

# Script generated for node accelerometer_landing
accelerometer_landing_node1791299533549 = glueContext.create_dynamic_frame.from_catalog(database="yared", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1791299533549")

# Script generated for node SQL Query
SqlQuery1876 = '''
SELECT a.*
FROM accelerometer_landing a
INNER JOIN customer_trusted c
    ON a.user = c.email
'''
SQLQuery_node1791299580358 = sparkSqlQuery(glueContext, query = SqlQuery1876, mapping = {"customer_trusted":customer_trusted_node1791299534692, "accelerometer_landing":accelerometer_landing_node1791299533549}, transformation_ctx = "SQLQuery_node1791299580358")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1791299580358, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1791299358593", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1791299758858 = glueContext.getSink(path="s3://yared-gomez/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1791299758858")
AmazonS3_node1791299758858.setCatalogInfo(catalogDatabase="yared",catalogTableName="accelerometer_trusted")
AmazonS3_node1791299758858.setFormat("glueparquet", compression="snappy")
AmazonS3_node1791299758858.writeFrame(SQLQuery_node1791299580358)
job.commit()
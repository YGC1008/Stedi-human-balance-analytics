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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1791306521151 = glueContext.create_dynamic_frame.from_catalog(database="yared", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1791306521151")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1791306519507 = glueContext.create_dynamic_frame.from_catalog(database="yared", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1791306519507")

# Script generated for node SQL Query
SqlQuery1612 = '''
SELECT
    s.sensorReadingTime,
    s.serialNumber,
    s.distanceFromObject,
    a.user,
    a.x,
    a.y,
    a.z
FROM step_trainer_trusted s
INNER JOIN accelerometer_trusted a
    ON s.sensorReadingTime = a.timeStamp
'''
SQLQuery_node1791306572228 = sparkSqlQuery(glueContext, query = SqlQuery1612, mapping = {"accelerometer_trusted":accelerometer_trusted_node1791306521151, "step_trainer_trusted":step_trainer_trusted_node1791306519507}, transformation_ctx = "SQLQuery_node1791306572228")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1791306572228, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1791306488603", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1791306616148 = glueContext.getSink(path="s3://yared-gomez/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1791306616148")
AmazonS3_node1791306616148.setCatalogInfo(catalogDatabase="yared",catalogTableName="machine_learning_curated")
AmazonS3_node1791306616148.setFormat("glueparquet", compression="snappy")
AmazonS3_node1791306616148.writeFrame(SQLQuery_node1791306572228)
job.commit()
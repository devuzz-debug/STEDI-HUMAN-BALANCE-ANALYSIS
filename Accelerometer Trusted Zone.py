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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1757930205863 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://my-glue-demo-12345/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1757930205863")

# Script generated for node Customer Trusted
CustomerTrusted_node1757930207284 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://my-glue-demo-12345/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1757930207284")

# Script generated for node Join
Join_node1757930854379 = Join.apply(frame1=AccelerometerLanding_node1757930205863, frame2=CustomerTrusted_node1757930207284, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1757930854379")

# Script generated for node SQL Query
SqlQuery3661 = '''
select user,timestamp,x,y,z from myDataSource

'''
SQLQuery_node1758774305078 = sparkSqlQuery(glueContext, query = SqlQuery3661, mapping = {"myDataSource":Join_node1757930854379}, transformation_ctx = "SQLQuery_node1758774305078")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758774305078, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1757927515327", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1757931279928 = glueContext.getSink(path="s3://my-glue-demo-12345/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1757931279928")
AccelerometerTrusted_node1757931279928.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1757931279928.setFormat("json")
AccelerometerTrusted_node1757931279928.writeFrame(SQLQuery_node1758774305078)
job.commit()
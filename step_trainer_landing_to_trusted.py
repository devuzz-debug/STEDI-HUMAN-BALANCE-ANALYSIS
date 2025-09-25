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

# Script generated for node Customer Curated
CustomerCurated_node1758797964042 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://my-glue-demo-12345/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1758797964042")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1758797961922 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://my-glue-demo-12345/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1758797961922")

# Script generated for node SQL Query
SqlQuery3578 = '''
select 
      s.sensorreadingtime,
      s.serialnumber,
      s.distancefromobject
from s 
inner join c 
on s.serialnumber = c.serialnumber
'''
SQLQuery_node1758775798581 = sparkSqlQuery(glueContext, query = SqlQuery3578, mapping = {"s":StepTrainerLanding_node1758797961922, "c":CustomerCurated_node1758797964042}, transformation_ctx = "SQLQuery_node1758775798581")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758775798581, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758775591922", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1758775966136 = glueContext.getSink(path="s3://my-glue-demo-12345/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1758775966136")
StepTrainerTrusted_node1758775966136.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1758775966136.setFormat("json")
StepTrainerTrusted_node1758775966136.writeFrame(SQLQuery_node1758775798581)
job.commit()
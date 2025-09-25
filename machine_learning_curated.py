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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1758776368776 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1758776368776")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1758776368102 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1758776368102")

# Script generated for node SQL Query
SqlQuery3777 = '''
select 
       a.user,
       a.timestamp,
       a.x,
       a.y,
       a.z,
       s.sensorreadingtime,
       s.serialnumber,
       s.distancefromobject
from s 
inner join a 
on s.sensorreadingtime = a.timestamp
'''
SQLQuery_node1758776458884 = sparkSqlQuery(glueContext, query = SqlQuery3777, mapping = {"s":StepTrainerTrusted_node1758776368776, "a":AccelerometerTrusted_node1758776368102}, transformation_ctx = "SQLQuery_node1758776458884")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758776458884, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758775591922", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1758776682543 = glueContext.getSink(path="s3://my-glue-demo-12345/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1758776682543")
MachineLearningCurated_node1758776682543.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1758776682543.setFormat("json")
MachineLearningCurated_node1758776682543.writeFrame(SQLQuery_node1758776458884)
job.commit()
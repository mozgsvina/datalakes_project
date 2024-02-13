import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1707821295832 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lakehouse-anya/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1707821295832",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1707821409870 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lakehouse-anya/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1707821409870",
)

# Script generated for node Join Data
SqlQuery0 = """
select * from stt
join at
on stt.sensorreadingtime = at.timestamp
"""
JoinData_node1707821466936 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "stt": StepTrainerTrusted_node1707821295832,
        "at": AccelerometerTrusted_node1707821409870,
    },
    transformation_ctx="JoinData_node1707821466936",
)

# Script generated for node Anonymize
Anonymize_node1707823187897 = DropFields.apply(
    frame=JoinData_node1707821466936,
    paths=["user"],
    transformation_ctx="Anonymize_node1707823187897",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1707821726398 = glueContext.getSink(
    path="s3://lakehouse-anya/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node1707821726398",
)
MachineLearningCurated_node1707821726398.setCatalogInfo(
    catalogDatabase="stedi_db", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node1707821726398.setFormat("json")
MachineLearningCurated_node1707821726398.writeFrame(Anonymize_node1707823187897)
job.commit()

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

# Script generated for node Customer Curated
CustomerCurated_node1707819671927 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lakehouse-anya/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1707819671927",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1707819608824 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lakehouse-anya/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1707819608824",
)

# Script generated for node Join
SqlQuery5 = """
select st.* from st
join cc
on st.serialNumber = cc.serialNumber
"""
Join_node1707819715307 = sparkSqlQuery(
    glueContext,
    query=SqlQuery5,
    mapping={
        "st": StepTrainerLanding_node1707819608824,
        "cc": CustomerCurated_node1707819671927,
    },
    transformation_ctx="Join_node1707819715307",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1707820116985 = glueContext.getSink(
    path="s3://lakehouse-anya/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1707820116985",
)
StepTrainerTrusted_node1707820116985.setCatalogInfo(
    catalogDatabase="stedi_db", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1707820116985.setFormat("json")
StepTrainerTrusted_node1707820116985.writeFrame(Join_node1707819715307)
job.commit()

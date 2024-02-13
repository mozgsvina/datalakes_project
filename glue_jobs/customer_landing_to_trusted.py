import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1707737069972 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lakehouse-anya/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1707737069972",
)

# Script generated for node Share With Research
ShareWithResearch_node1707737284230 = Filter.apply(
    frame=CustomerLanding_node1707737069972,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="ShareWithResearch_node1707737284230",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1707737312657 = glueContext.getSink(
    path="s3://lakehouse-anya/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1707737312657",
)
CustomerTrusted_node1707737312657.setCatalogInfo(
    catalogDatabase="stedi_db", catalogTableName="customer_trusted"
)
CustomerTrusted_node1707737312657.setFormat("json")
CustomerTrusted_node1707737312657.writeFrame(ShareWithResearch_node1707737284230)
job.commit()

import os
import sys
from pyspark.sql import SparkSession


def create_spark_session(app_name="DataTrustScoring"):
    """
    Initializes and returns a SparkSession configured for local execution
    with Delta Lake support.
    """

    # ----------------------------------------------------
    # Windows Hadoop Configuration
    # ----------------------------------------------------
    if sys.platform.startswith("win"):

        # Absolute path to Hadoop folder containing winutils.exe
        hadoop_home = "C:/jiya/hadoop"

        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["hadoop.home.dir"] = hadoop_home
        os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")

    # ----------------------------------------------------
    # Ensure PySpark uses the venv Python (3.11)
    # ----------------------------------------------------
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # ----------------------------------------------------
    # Create Spark Session
    # ----------------------------------------------------
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.driver.memory", "2g")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.sql.sources.commitProtocolClass",
                "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )

    # Reduce log verbosity
    spark.sparkContext.setLogLevel("ERROR")

    return spark
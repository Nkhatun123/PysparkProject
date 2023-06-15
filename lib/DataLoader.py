 from lib import ConfigLoader


def get_datasetOne_schema():
    schema = """id int,first_name string,last_name string,
        email string,country string"""
    return schema


def get_datasetTwo_schema():
    schema = """id int,btc_a string,cc_t string,
    cc_n long"""
    return schema


def read_datasetOne(spark, env):
    country_filter = ConfigLoader.get_data_filter(env, "country.filter")
    path1=ConfigLoader.get_path(env, "datasetone.path")
    return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(get_datasetOne_schema()) \
            .load(path1) \
            .where(country_filter)


def read_datasettwo(spark, env):
    path1=ConfigLoader.get_path(env, "datasettwo.path")
    return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(get_datasetTwo_schema()) \
            .load(path1)

def write_finaldf(spark,env):
    path1=ConfigLoader.get_path(env, "finaldataset.path")
    return spark.write.format("csv").option("header","true").save(path1)


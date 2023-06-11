spark-submit --master yarn --deploy-mode cluster \
--py-files lib.zip \
--files conf/proj.conf,conf/spark.conf,log4j.properties \
main.py qa 2023-06-12
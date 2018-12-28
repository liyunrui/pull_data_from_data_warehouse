"""
25 Dec 2018
Modified on 26 Dec 2018


1.collect all SKU data from the 5 categories: women fashion, men fashion, women shoes, men shoes, health & beauty
2.clean brand attribute
3.remove dirty brand attributes
4.remove low-frequency brands
5.add "no-brand" for SKUs without brand
6.split 80 rain 20 test data set by ctime
7.put the data & code in HDFS for sharing

@author: Ray

"""
import os
import findspark
findspark.init('/home/work/spark')
os.environ['PYSPARK_PYTHON'] = '/home/work/miniconda2/envs/py27/bin/python'
import argparse
import re
import sys
import pyspark
import json
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext, SparkSession
from pyspark.sql.functions import udf, collect_list,collect_set  # Creates a user defined function (UDF).
from pyspark.sql.types import StringType # specify the data type using the types from pyspark.sql.types for UDF
from pyspark.sql.functions import lower, col

def brand_clean(brand):
    """
    1.remove symbols except '&','-'
    2.remove brand with single character and numeric numbers onlys.
    
    test_string: 
        maple syrup ==> $20.99?!! SK-2 A&F L'roeal 
    output: 
        maple syrup 20 99 SK-2 A&F Lroeal

    """
    try:
        if len(brand) <= 1:
            # remove brand with single character
            brand = "None"
        elif brand.isdigit() == True:
            # remove brand with only numbers
            brand = "None"
        else:
            # remove symbols except '&','?','.','!' 
            brand = re.sub(r"[^a-zA-Z0-9&-']+",' ', brand)
            brand = re.sub(r"'",'', brand)
        return brand
    except TypeError:
        return "None"

def no_brand_add(brand):
    """
    replace SKUs without brand in terms of the seller's selective with "no-brand".
    
    """
    trash_from_eyeball_checking = {"no merk","impor","import","lokal", 
                               "tidak ada merek", "tidak ada merk", 
                               "no brand","lainnya","branded"}
    try:
        if brand.lower() in trash_from_eyeball_checking:
            return "no-brand"
        else:
            return brand
    except TypeError:
        return brand

    
def normalize_brand(brand):
    """
    normalize brand variations. e.g. charles and keith, charles & keith -> charles&keith
    
    """
    try:
        brand = brand.lower()
        brand = re.sub(r'\s+&\s+', '&', brand)
        brand = re.sub(r'\s+and\s+', '&', brand)
        brand = re.sub("louis vuitton", "lv", brand)
        brand = "new balance" if brand=='nb' else brand
        brand = re.sub("x s m l", "no-brand", brand)
        return brand
    except TypeError:
        return brand

def process_keyword(keyword):
    """ process a keyword
    1. lowercase
    2. keep only letter, number and punctuation
    3. remove certain character at the begining/end
    4. remove repeated character
    :input: a string
    :output: a string
    """
    try:
        pn = keyword.lower()  # .decode("utf-8", errors='ignore')
        pn = re.sub(ur'[^\u4e00-\u9fff a-zA-Z0-9$%&/\-+:]', ' ', pn)
        preceding_trailing_chars = "$+/%&-.:"
        seen = set()
        results = []
        for word in pn.split():
            word = word.strip(preceding_trailing_chars)
            if word and word not in seen:
                results.append(word)
                seen.add(word)
        res = " ".join(results)
        return res
    except:
        return ""


if __name__ == "__main__":
    # The only difference is that with PySpark UDFs I have to specify the output data type.
    clean_brand_udf = udf(lambda input_brand: brand_clean(input_brand), StringType())
    no_brand_add_udf = udf(lambda input_brand: no_brand_add(input_brand), StringType())
    normalize_brand_udf = udf(lambda input_brand: normalize_brand(input_brand), StringType())
    process_title_udf = udf(lambda title: process_keyword(title), StringType())

    #-----------------------
    # standalizing process
    #-----------------------
    name = 'pull_brand'
    port = 1992
    SPARK_SERVICE = None

    SPARK_CONF = SparkConf() \
        .set("spark.executor.memory", '250g') \
        .set("spark.driver.memory", '250g') \
        .set("spark.driver.maxResultSize", '250g') \
        .set("spark.executor.instances", '60') \
        .set("spark.executor.cores", '4') \
        .set("spark.sql.crossJoin.enabled", True) \
        .set("spark.cores.max", '4') \
        .set('spark.yarn.queue', 'dev')  # .set('spark.yarn.queue', 'ds-regular')

    sc = SparkContext(SPARK_SERVICE, appName=name, conf=SPARK_CONF)
    spark = SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()

    # setting
    main_category = ['Women Shoes','Men Shoes','Health & Beauty','Women Clothes','Men Clothes']

    #--------------------
    # main
    #--------------------
    spark.sql('use shopee')  # For example. it's for checking shoope.any_table_name

    query = """
    SELECT itemid, string(item_name) as item_name, decode(unbase64(brand), "utf-8") as brand,
    item_ctime, main_category, sub_category, level3_category, main_cat, sub_cat, level3_cat 
    FROM item_profile
    WHERE (country = 'ID') and (item_status = 1) and (main_category IN ({}))
    """.format(repr(main_category)[1:-1])

    print ("query", query)

    df = spark.sql(query)
    #------------------
    #0. preprocessing
    #------------------
    df = df.drop_duplicates(subset = ['itemid'])
    df = df.withColumn("item_name", process_title_udf("item_name")) 
    df.show(10, truncate = False)
    print ('Successfully pull data')

    #------------------------
    # 1.remove dirty brand attributes
    #------------------------
    df = df.withColumn("brand", clean_brand_udf("brand")) 
    df = df.filter(df.brand != 'None')
    df = df.withColumn("brand", normalize_brand_udf("brand")) 
    df = df.dropna(subset = ['brand'])
    df.show(10, truncate = False)
    print ('Successfully clean brand field')

    #------------------------
    # 2.replace SKUs without brand in terms of the seller's selective with "no-brand".
    #------------------------
    df = df.withColumn("brand", no_brand_add_udf("brand")) 

    #------------------------
    # 3.count requency of brands
    #------------------------
    brand_count_df = df.withColumn('brand', lower(col('brand'))).groupBy('brand').count()
    df = df.join(brand_count_df, on = 'brand')
    print ('Successfully count requency of brands')


    #------------------------
    # 4.split 80% train 20% test data set by ctime
    #------------------------
    n = df.count() # 90 millions
    n_train = int(n*0.8)
    n_test = n-n_train

    # sort ascending and take first 100 rows for df1
    df1 = df.sort('item_ctime', ascending = True).limit(n_train)
    # sort descending and take 400 rows for df2
    df2 = df.sort('item_ctime', ascending=False).limit(n_test)
    print ('Successfully split train/test data for raw data')

    # #-----------------------
    # # put file into local
    # #-----------------------
    # local_folder = '/data/brand_classification/brand_classification_raw_train.csv'
    # df1.toPandas().to_csv(local_folder, encoding = 'utf-8', index = False)
    # local_folder = '/data/brand_classification/brand_classification_raw_test.csv'
    # df2.toPandas().to_csv(local_folder, encoding = 'utf-8', index = False)

    #-----------------------
    # put file into hadoop(raw)
    #-----------------------
    hadoop_folder_1 = '/user/yunrui.li/brand_classification_raw/train'
    df1.write.csv(hadoop_folder_1, mode='overwrite', header=True)
    hadoop_folder_2 = '/user/yunrui.li/brand_classification_raw/test'
    df2.write.csv(hadoop_folder_2, mode='overwrite', header=True)

    #------------------------
    # 5.remove low-frequency brands
    #------------------------
    df = df.filter("count > 500")
    #------------------------
    # split 80% train 20% test data set by ctime
    #------------------------
    n = df.count() # 90 millions
    n_train = int(n*0.8)
    n_test = n-n_train

    # sort ascending and take first 100 rows for df1
    df1 = df.sort('item_ctime', ascending = True).limit(n_train)
    # sort descending and take 400 rows for df2
    df2 = df.sort('item_ctime', ascending=False).limit(n_test)
    print ('Successfully split train/test data for cleaned data')


    #-----------------------
    # put file into hadoop(cleaned)
    #-----------------------
    hadoop_folder_1 = '/user/yunrui.li/brand_classification_cleaned/train'
    df1.write.csv(hadoop_folder_1, mode='overwrite', header=True)
    hadoop_folder_2 = '/user/yunrui.li/brand_classification_cleaned/test'
    df2.write.csv(hadoop_folder_2, mode='overwrite', header=True)


    print("Finish pyspark part and put file into hadoop")

    # #-----------------------
    # # put file into local
    # #-----------------------
    # local_folder = '/data/brand_classification/brand_classification_train.csv'
    # df1.toPandas().to_csv(local_folder, encoding = 'utf-8', index = False)
    # local_folder = '/data/brand_classification/brand_classification_test.csv'
    # df2.toPandas().to_csv(local_folder, encoding = 'utf-8', index = False)

    # print("Put file into local")

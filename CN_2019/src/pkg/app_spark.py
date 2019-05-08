print('aaaaaaa')


from flask import Flask, jsonify, request, render_template
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine
import re

import pyspark
from pyspark.sql import SparkSession

import io
from contextlib import redirect_stdout

#from pyspark_dist_explore import hist

limit = 10**6
#set up app
app = Flask(__name__, static_folder = "static")

print('aaaaaaa')

####################################################################
#SPARK
####################################################################
'''
from pyspark.sql.session import SparkSession
spark = SparkSession \
    .builder \
    .config("spark.driver.extraClassPath","mysql.jar") \
    .appName("Python Spark SQL data source example") \
    .getOrCreate()
    
jdbcDF = spark.read.format("jdbc") \
    .option("url", "jdbc:sqlserver://localhost:3306/databaseName=netflix_db") \
    .option("dbtable", "netflix_data") \
    .option("user", "root") \
    .option("password", "red").load()
print(jdbcDF)
'''
###pushdown_query = "select * from netflix_data LIMIT 10;"
###df = spark.read.jdbc(url=url, dbtable=pushdown_query, properties=connectionProperties)
#sqlContext=SQLContext(sc)
#df=sqlContext.read.jdbc(url=url, table=pushdown_query, properties=properties)
###print(df)

####################################################################
#SPARK
####################################################################
'''
from pyspark.sql import SQLContext, Row
from pyspark import SparkConf, SparkContext

#conf = SparkConf().setAppName("ReadSQLServerData")
#sc = SparkContext(conf=conf)
#query = "(SELECT top 10 * from users) as users"
#sqlctx = SQLContext(sc)
spark = SparkSession \
.builder \
.appName("Python Spark SQL basic example") \
.config("spark.driver.extraClassPath","A:\ProgramData\Anaconda3\envs\py3\Lib\site-packages\pyspark\jars") \
.getOrCreate()

#df = sqlctx.read.format("jdbc").options\
#    (url="jdbc:sqlserver://mssqlserver:1433;database=user_management;user=pyspark;password=pyspark", dbtable=query).load()
jdbcDF = spark.read \
    .format("jdbc") \
    .option('driver', 'com.mysql.jdbc.Driver')\
    .option("url", "jdbc:MariaDB:dbserver") \
    .option("dbtable", "schema.tablename") \
    .option("user", "root") \
    .option("password", "red") \
    .load()
    '''
####################################################################
#central_tendency_1  #tested, working
####################################################################
def get_data():
    # Parameters
    engine = create_engine("mysql+pymysql://root:red@localhost/netflix_db")
    sql = "select * from netflix_data limit %s" % limit
    df = pd.read_sql(sql, engine, columns=['user_id', 'rating', 'year', 'movie_id'])
    df['year'] = pd.DatetimeIndex(df['year']).year
    return df

def get_data_spark():
    spark = SparkSession.builder.appName('pandasToSparkDF').getOrCreate()
    return spark.createDataFrame(get_data())


'''
all the math that goes into the summary can be done using spark
Pyspark: how are dataframe describe() and summary() implemented?
https://stackoverflow.com/questions/50793165/pyspark-how-are-dataframe-describe-and-summary-implemented
'''
@app.route("/summary/rating-year", \
    methods=['GET', 'POST'])
def central_tendency_1():
    df = get_data_spark()
    with io.StringIO() as buf, redirect_stdout(buf):
        df.describe().show()
        summary = buf.getvalue()    
    print(summary)
    summary = re.split("\| \+|\+ \||\| \|", summary)
    print(summary)
    summary = summary[0].replace("<br>", "\n")
    print(summary)
    summary = '<br>'.join(summary)
    #print(summary)
    
    #produce the html
    begin = \
    '''
    <!DOCTYPE html>
    <html>
    <body>

    '''
    end = \
    '''
    </body>
    </html>

    '''
    mid = \
    '''
    <h2>summary</h2>
    %s
    '''%(summary,)

    with open('templates/summary.html', "w") as text_file:
        text_file.write(begin + mid + end)
    
    return render_template('summary.html')

####################################################################
#dispersion_1  #tested, working
####################################################################
def plot_hist(df, column_name):
    fig, ax = plt.subplots()
    df.hist('%s'%column_name, ax=ax)
    fig.savefig('static/histogram_%s.png'%column_name)

@app.route("/histogram/rating-year", \
    methods=['GET', 'POST'])
def dispersion_1():
    rating = 'rating'
    year = 'year'
    #produce the plots
    df = get_data()
    plot_hist(df, rating)
    plot_hist(df, year)
    #produce the html
    begin = \
    '''
    <!DOCTYPE html>
    <html>
    <body>

    '''
    end = \
    '''
    </body>
    </html>

    '''
    img_year = \
    '''
    <h2>%s histogram</h2>
    <img src="/static/histogram_%s.png">

    '''%(year, year)
    img_rating = \
    '''
    <h2>%s histogram</h2>
    <img src="/static/histogram_%s.png">

    '''%(rating, rating)

    with open('templates/dispersion_1.html', "w") as text_file:
        text_file.write(begin + img_year + img_rating + end)

    return render_template('dispersion_1.html')

####################################################################
#dispersion_2 #tested, working
####################################################################
def plot_boxplot(df, column_name):
    #produce plots
    fig, ax = plt.subplots()
    df.boxplot('%s'%column_name, ax=ax)
    fig.savefig('static/boxplot_%s.png'%column_name)

@app.route("/boxplot/rating-year", \
    methods=['GET', 'POST'])
def dispersion_2():
    df = get_data()
    plot_boxplot(df, 'rating')
    plot_boxplot(df, 'year')

    #produce the html
    rating = 'rating'
    year = 'year'
    begin = \
    '''
    <!DOCTYPE html>
    <html>
    <body>

    '''
    end = \
    '''
    </body>
    </html>

    '''
    img_year = \
    '''
    <h2>%s boxplot</h2>
    <img src="/static/boxplot_%s.png">

    '''%(year, year)
    img_rating = \
    '''
    <h2>%s boxplot</h2>
    <img src="/static/boxplot_%s.png">

    '''%(rating, rating)

    with open('templates/dispersion_2.html', "w") as text_file:
        text_file.write(begin + img_year + img_rating + end)

    return render_template('dispersion_2.html')

if __name__ == '__main__':
    app.run(debug=True)
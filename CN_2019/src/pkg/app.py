from flask import Flask, jsonify, request, render_template
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine

limit = 10**6
#set up app
app = Flask(__name__, static_folder = "static")

####################################################################
#central_tendency_1  #tested, working
####################################################################
def get_data():
    # Parameters #ip: 35.233.54.149
    engine = create_engine("mysql+pymysql://root:red@localhost/netflix_db")
    sql = "select * from netflix_data limit %s" % limit
    df = pd.read_sql(sql, engine, columns=['user_id', 'rating', 'year', 'movie_id'])
    return df


@app.route("/summary/rating-year", \
    methods=['GET', 'POST'])
def central_tendency_1():
    df = get_data()
    df['year'] = pd.DatetimeIndex(df['year']).year
    df.describe().round(2).to_html('templates/summary.html')
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
    df['year'] = pd.DatetimeIndex(df['year']).year
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
    df['year'] = pd.DatetimeIndex(df['year']).year
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
import time
import datetime as dt
from datetime import date, timedelta
import psycopg2
import os
import pandas as pd
from dateutil.parser import parse
from selenium import webdriver
from bs4 import BeautifulSoup


def get_metrics(url):

    '''
    Opens a browser, grabs all the relevant data. Argument takes a url.
    Function opens a webdriver, gets info from that url, sleeps a tiny bit to
    give time for the page to load, finds the appropriate class in the html/css/js
    and appends the right elements to a list. Returns a list of strings that
    represents percentages.
    '''
    print("Scraping daily MBTA performance metrics for the {} line.".format(url[81:-1]))

    driver = webdriver.PhantomJS()
    driver.get(url)
    time.sleep(10)
    elements = driver.find_elements_by_xpath("//span[@class='ng-binding ng-scope']")
    empty_list = []

    for i in elements:
        empty_list.append(i.text)
    driver.close()

    date = parse(empty_list[1])

    metrics = []
    for percent in empty_list[2:5]:
        metrics.append(percent)

    return metrics

def get_date(url):

    '''
    Grabs the date the MBTA uses on their website. Uses the same framework as
    getting the on time percentages, but just looking at different tags in
    selenium.
    '''

    driver = webdriver.PhantomJS()
    driver.get(url)
    time.sleep(10)
    elements = driver.find_elements_by_xpath("//span[@class='ng-binding ng-scope']")
    empty_list = []

    for i in elements:
        empty_list.append(i.text)
    driver.close()

    date = parse(empty_list[1])

    return date

def get_targets_numbers(url):

    '''
    The get_target_numbers() function takes a url, uses selenium to download
    the webpage, then uses BeautifulSoup to parse out the route, actual
    reliability percentage, and target reliability percentage.
    It returns a formatted dataframe with the date the dataframe was updated,
    the day of the metric_date (always yesterday), the Route (Bus, Bus Lines,
    T train line etc), actual percent, and target percent.
    '''

    print('Scraping MTBA performance metrics for ACTUAL performance, and TARGET performance.')

    # Open a PhantomJS browser, navfor index, row in metrics_dataframe.iterrows():
    # navigate to the url, let the page load, get the
    # page source, and close the browser.
    driver = webdriver.PhantomJS()
    driver.get(url)
    time.sleep(10)
    html = driver.page_source
    driver.close()

    # Parse the elements using BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    # Get all elements associated with route and percentage targets (target and
    # actual percentages)
    route_elements = soup.findAll('td', {'class': 'categoryTd ng-binding ng-scope'})
    target_elements = soup.findAll('td', {'ng-repeat':'series in metric.series'})

    # Create list containers for route and targets.
    route_list = []
    target_list = []

    # Iterate on the elements, strip them of any tags, append them to a list.
    for i in route_elements:
        route_list.append(i.get_text().strip())

    # Actual Percentages reside in EVEN number elements in the target_elements
    # list, while target percentages reside in ODD number elements in the target
    # elements list. Parse out each and place the into their own lists.
    actual_reliability_elements = target_elements[0::2]
    target_reliability_elements = target_elements[1::2]

    # Create containers for the reliability metrics
    actual_reliability_list = []
    target_reliability_list = []

    # Iterate over the actual and target elements, place them into the created
    # list containers
    for i in actual_reliability_elements:
        actual_reliability_list.append(i.get_text().strip())

    for i in target_reliability_elements:
        target_reliability_list.append(i.get_text().strip())

    # Updated date is always today, the mbtas updated date is always yesterday.
    today = date.today()
    yesterday = date.today() - timedelta(1)

    # Populate a dataframe, then reorder the columns so they look nice.
    dataframe = pd.DataFrame({

        'route': route_list,
        'actual': actual_reliability_list,
        'target': target_reliability_list,

        })

    dataframe['metric_date'] = yesterday
    dataframe['date_updated'] = today

    dataframe = dataframe[['date_updated', 'metric_date', 'route', 'actual', 'target']]

    return dataframe

def format_dataframe_vars(dataframe):

    '''
    Converts string values (e.g., 90%) to numeric (double, specifically).
    The converts numbers to a decimal value (e.g., .90). Takes one argument,
    a data frame of performance metrics.
    Specifically used as a helper function for arrange_and_format().
    '''

    print('Formatting metrics data frame...')

    dataframe['target'] = dataframe['target'].map(lambda x: x.rstrip('%'))
    dataframe['past_day'] = dataframe['past_day'].map(lambda x: x.rstrip('%'))
    dataframe['past_7'] = dataframe['past_7'].map(lambda x: x.rstrip('%'))
    dataframe['past_30'] = dataframe['past_30'].map(lambda x: x.rstrip('%'))

    dataframe['target'] = pd.to_numeric(dataframe['target'])
    dataframe['past_day'] = pd.to_numeric(dataframe['past_day'])
    dataframe['past_7'] = pd.to_numeric(dataframe['past_7'])
    dataframe['past_30'] = pd.to_numeric(dataframe['past_30'])

    dataframe['target'] = dataframe['target'].map(lambda x: x / 100)
    dataframe['past_day'] = dataframe['past_day'].map(lambda x: x / 100)
    dataframe['past_7'] = dataframe['past_7'].map(lambda x: x / 100)
    dataframe['past_30'] = dataframe['past_30'].map(lambda x: x / 100)

    return(dataframe)

def arrange_and_format(t_lines, past_day, past_7, past_30, mbta_date, target_dataframe):

    '''
    Takes lists of T route, metrics from the past day, week, month, and organizes
    them in the required way.
    Also merges target percentage dataframe.
    '''

    dataframe = pd.DataFrame({

        'route': t_lines,
        'past_day': past_day,
        'past_7': past_7,
        'past_30': past_30
    })

    dataframe['metric_date'] = mbta_date
    dataframe['date_updated'] = dt.date.today()

    dataframe = dataframe.merge(target_dataframe,  how='inner', on=['route'], suffixes=('_x', '_y'))

    dataframe = dataframe[['date_updated', 'metric_date', 'route', 'target', 'past_day', 'past_7', 'past_30']]

    dataframe = format_dataframe_vars(dataframe)

    return dataframe

def write_to_psql(dataframe, connection_string, table):

    '''
    Write data to PSQL server. Takes a data frame (returned from get_targets_numbers
    , and format_dataframe_vars) a connection_string, and what table to write to.
    '''

    print('Writing data to Postgres.')

    # Open a psycopg2 connection, define a cursor and table.
    conn = psycopg2.connect(connection_string)
    cur = conn.cursor()
    table = table

    # Define the query to insert into the target SQL table.
    insert_query = 'INSERT INTO ' + table + ' VALUES(%s, %s, %s, %s, %s, %s, %s)'

    # Iterate over DF rows, using the insert query to insert each piece
    # of data row by row.
    for index, row in dataframe.iterrows():
        cur.execute(insert_query, (row['metric_date'], row['route'],
                                   row['target'], row['past_day'],
                                   row['past_7'], row['past_30'],
                                   row['date_updated']))

    # Commit the changes, close the connection.
    conn.commit()
    conn.close()

    print("All rows from dataframe uploaded to Postgres.")

if __name__ == '__main__':

    # Define T Line Urls
    red_url = 'http://www.mbtabackontrack.com/performance/index.html#/detail/reliability/subway/red/'
    blue_url = 'http://www.mbtabackontrack.com/performance/index.html#/detail/reliability/subway/blue/'
    green_url = 'http://www.mbtabackontrack.com/performance/index.html#/detail/reliability/subway/green/'
    orange_url = 'http://www.mbtabackontrack.com/performance/index.html#/detail/reliability/subway/orange/'
    bus_url = 'http://www.mbtabackontrack.com/performance/index.html#/detail/reliability/bus//'
    commuterrail_url = 'http://www.mbtabackontrack.com/performance/index.html#/detail/reliability/commuter_rail//'

    # Get the T Line Metrics
    red_line = get_metrics(red_url)
    blue_line = get_metrics(blue_url)
    green_line = get_metrics(green_url)
    orange_line = get_metrics(orange_url)
    bus = get_metrics(bus_url)
    commuter_rail = get_metrics(commuterrail_url)

    # Arrange into lists that are organized by date rather than Line
    mbta_date = get_date(red_url)
    past_day = [red_line[0], blue_line[0], green_line[0], orange_line[0], bus[0], commuter_rail[0]]
    past_7 = [red_line[1], blue_line[1], green_line[1], orange_line[1], bus[1], commuter_rail[1]]
    past_30 = [red_line[2], blue_line[2], green_line[2], orange_line[2], bus[2], commuter_rail[2]]

    # Define the T Lines
    t_lines = ['Red Line', 'Blue Line', 'Green Line', 'Orange Line', 'Bus', 'Commuter Rail']

    # Get Actual and Target Metrics. Takes the targets_dataframe, finds all route labeled
    # Red Line, Orange Line, Green Line etc, and subsets them into a new data frame.
    # Drops unnecessary columns.
    # NOTE: This part of the scraper gets more (interesting data) than necessary,
    # Perhaps ETL this in the future?
    target_percentage_url = 'http://www.mbtabackontrack.com/performance/index.html#/detail/reliability///'
    targets_dataframe = get_targets_numbers(target_percentage_url)
    targets_dataframe = targets_dataframe[(targets_dataframe['route'] == 'Red Line') | (targets_dataframe['route'] == 'Blue Line') | (targets_dataframe['route'] == 'Green Line') | (targets_dataframe['route'] == 'Orange Line') | (targets_dataframe['route'] == 'Bus') | (targets_dataframe['route'] == 'Commuter Rail')]
    targets_dataframe = targets_dataframe.drop(['date_updated', 'metric_date', 'actual'], axis = 1)

    # Create a data frame of junk and arrange it appropriately
    metrics_dataframe = arrange_and_format(t_lines, past_day, past_7, past_30, mbta_date, targets_dataframe)
    metrics_dataframe = metrics_dataframe[['metric_date', 'route', 'target', 'past_day', 'past_7', 'past_30', 'date_updated']]

    # Write results to postgresql. Defines login info from environment variables,
    # connects to SQL server using write_to_psql() function. Dumps all scraped data to it.

    env_var_dict = dict()

    env_var_dict['database_hostname'] = os.environ.get('POSTGRES_IP')
    env_var_dict['database_port'] = os.environ.get('POSTGRES_PROD_PORT')
    env_var_dict['database_name'] = os.environ.get('POSTGRES_PROD_DB')
    env_var_dict['database_user'] = os.environ.get('POSTGRES_PROD_USER')
    env_var_dict['database_pass'] = os.environ.get('POSTGRES_PROD_PASS')

    connection_string = "dbname='{}' user='{}' host='{}' password='{}' port={}".format(env_var_dict['database_name'],
                                                                                       env_var_dict['database_user'],
                                                                                       env_var_dict['database_hostname'],
                                                                                       env_var_dict['database_pass'],
                                                                                       int(env_var_dict['database_port']))

    table = 'table_name'
write_to_psql(metrics_dataframe, connection_string, table)

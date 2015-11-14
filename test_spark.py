 #!/usr/bin/env python

"""This script can be used to compute metrics based on a lot of events using Spark"""

__author__ = 'flo'

import collections
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import datediff, explode, udf, unix_timestamp, second, minute
from datetime import datetime, timedelta


def get_date(timeframe):
    """
    returns the start date of the period
    input: a period (string)
    output: a date (string)
    """
    if timeframe == "today":
        timeframe_is = str(datetime.today().date())
    elif timeframe == "this_1_weeks":
        timeframe_is = str(datetime.today().date() - timedelta(days=7))
    elif timeframe == "this_1_months":
        timeframe_is = str(datetime.today().date() - timedelta(days=365.25/12))
    elif timeframe == "this_1_years":
        timeframe_is = str(datetime.today().date() - timedelta(days=365.25))
    elif timeframe == "all":
        timeframe_is = str(datetime(1960, 1, 1).date())
    return timeframe_is


def get_number_of_searches(timeframe, partner, premium):
    """
    returns the number of searches by a partner over a time period ending today.
    input: a period (string), a partner (string), a spark dataframe
    output: int
    """
    timeframe_is = get_date(timeframe)
    result = (premium.filter(premium.keen.timestamp >= timeframe_is)
              .filter(premium.search_info.partner_id == partner)
              .count())
    return result



def get_farekeep_sold(timeframe, partner, purchase):
    """
    returns the number of farekeeps sold by a partner over a time period ending today.
    input: a period, a partner
    output: int
    """
    timeframe_is = get_date(timeframe)
    result = (purchase.filter(purchase.keen.timestamp >= timeframe_is)
              .filter(purchase.search_info.partner_id == partner)
              .agg({'purchase.quantity':'sum'}).collect()[0][0])
    return result


def get_net_income(timeframe, partner, claim):
    """
    returns the net income (revenue - loss) for a partner over a time period ending today
    input: a period, a partner
    output: a float
    """
    timeframe_is = get_date(timeframe)
    result = (claim.filter(claim.keen.timestamp >= timeframe_is)
              .filter(claim.search_info.partner_id == partner)
              .agg({'claim.PnL_total':'sum'}).collect()[0][0])
    return result


def get_coverage(timeframe, partner, premium):
    """
    returns the minimum coverage i.e the number of searches that shows at least
    one impressions for a partner over a time period ending today
    input: a period, a partner
    output: a float
    """
    timeframe_is = get_date(timeframe)
    result = round(float(premium.filter(premium.keen.timestamp >= timeframe_is).filter(premium.search_info.partner_id == partner).filter(premium.premium.flights_with_premium > 0).count()) / premium.filter(premium.keen.timestamp >= timeframe_is).filter(premium.search_info.partner_id == partner).count(), 4)
    return result


def get_conversion(timeframe, partner, premium, quote, purchase):
    """
    returns the funnel analysis i.e the conversion rates at each stage of the process 
    for a partner over a time period ending today
    input: a period, a partner
    output: a dictionary with the absolute number and the proportion
    """
    result = {}
    timeframe_is = get_date(timeframe)
    impressions = (premium.filter(premium.keen.timestamp >= timeframe_is)
                   .filter(premium.search_info.partner_id == partner)
                   # .filter(premium.search_info.search_id != 'NaN')
                   .count())
    quotes = (quote.filter(quote.keen.timestamp >= timeframe_is)
              .filter(quote.search_info.partner_id == partner)
              # .filter(quote.search_info.search_id != 'NaN')
              .count())
    purchases = (purchase.filter(purchase.keen.timestamp >= timeframe_is)
                 .filter(purchase.search_info.partner_id == partner)
                 # .filter(purchase.search_info.search_id != 'NaN')
                 .count())
    result['searches'] = {"absolute": impressions, "relative": 1.0}
    result['quotes'] = {"absolute": quotes, "relative": round(float(quotes)/impressions, 4)}
    result['purchases'] = {"absolute": purchases, "relative": round(float(purchases)/impressions, 4)}
    return result    


def get_carrier_profit(timeframe, partner, purchase):
    """
    returns the profit by carrier for a partner over a time period ending today.
    input: a period (string), a partner(string)
    output: a dictionary with the absolute number and the proportion
    """
    result = {}
    timeframe_is = get_date(timeframe)
    profit = (purchase.filter(purchase.keen.timestamp >= timeframe_is)
              .filter(purchase.search_info.partner_id == partner)
              .groupBy('flight.outbound.carrier').sum('purchase.total_amount').collect())
    tot = sum([row[1] for row in profit])
    for row in profit:
        result[row[0]] = {"absolute": row[1], "relative":round(float(row[1])/tot, 4)}
    return result


def get_carrier_loss(timeframe, partner, claim):
    """
    returns the profit by carrier for a partner over a time period ending today.
    input: a period (string), a partner(string)
    output: a dictionary with the absolute number and the proportion
    """
    result = {}
    timeframe_is = get_date(timeframe)
    loss = (claim.filter(claim.keen.timestamp >= timeframe_is)
            .filter(claim.search_info.partner_id == partner)
            .groupBy('flight.outbound.carrier')
            .sum('claim.payout_total').collect())
    tot = sum([row[1] for row in loss])
    for row in loss:
        result[row[0]] = {"absolute": row[1], "relative":round(float(row[1])/tot, 4)}
    return result


def get_popular_origins(timeframe, partner, top, premium):
    """
    returns the [top] most popular origins for a partner over a time period ending today.
    input: a period (string), a partner(string), the number of destinations you want to output (integer)
    output: a dictionary with the origins and the number of searches
    """
    result = collections.OrderedDict()
    timeframe_is = get_date(timeframe)
    origins = (premium.filter(premium.keen.timestamp >= timeframe_is)
               .filter(premium.search_info.partner_id == partner)
               .groupBy('flight.origin')
               .count().collect())
    origins.sort(key=lambda x: x[1], reverse=True)
    for row in origins[:top]:
        result[row[0]] = row[1]
    return result


def get_popular_destinations(timeframe, partner, top, premium):
    """
    returns the [top] most popular destinations for a partner over a time period ending today.
    input: a period (string), a partner(string), the number of destinations you want to output (integer)
    output: a dictionary with the origins and the number of searches
    """
    result = collections.OrderedDict()
    timeframe_is = get_date(timeframe)
    destinations = (premium.filter(premium.keen.timestamp >= timeframe_is)
                    .filter(premium.search_info.partner_id == partner)
                    .groupBy('flight.destination')
                    .count().collect())
    destinations.sort(key=lambda x: x[1], reverse=True)
    for row in destinations[:top]:
        result[row[0]] = row[1]
    return result


def get_destination_profit(timeframe, partner, top, purchase):
    """
    returns the [top] most profitable destinations for a partner over a time period ending today.
    input: a period (string), a partner(string), the number of destinations you want to output (integer)
    output: a dictionary with the origins and the number of searches
    """
    result = collections.OrderedDict()
    timeframe_is = get_date(timeframe)
    destinations = (purchase.filter(purchase.keen.timestamp >= timeframe_is)
                    .filter(purchase.search_info.partner_id == partner)
                    .groupBy('flight.outbound.destination')
                    .sum('purchase.total_amount').collect())
    destinations.sort(key=lambda x: x[1], reverse=True)
    for row in destinations[:top]:
        result[row[0]] = row[1]
    return result


def get_destination_loss(timeframe, partner, top, claim):
    """
    returns the [top] least profitable destinations for a partner over a time period ending today.
    input: a period (string), a partner(string), the number of destinations you want to output (integer)
    output: a dictionary with the origins and the number of searches
    """
    result = collections.OrderedDict()
    timeframe_is = get_date(timeframe)
    destinations = (claim.filter(claim.keen.timestamp >= timeframe_is)
                    .filter(claim.search_info.partner_id == partner)
                    .groupBy('flight.outbound.destination')
                    .sum('claim.payout_total').collect())
    destinations.sort(key=lambda x: x[1], reverse=True)
    for row in destinations[:top]:
        result[row[0]] = row[1]
    return result


def get_tud_profit(timeframe, partner, purchase):
    """
    returns the distribution of profit by time until departure for a partner over a time period ending today.
    input: a period (string), a partner (string), a spark dataframe
    output: a dictionary with the tud bucket and the values associated
    """
    keys = [0, 21, 28, 35, 42, 49, 56, 200] # the predefined bucket less than 3 weeks, btw 3 and 4 weeks, ...
    result = collections.OrderedDict()
    result = {key: 0.0 for key in keys}
    timeframe_is = get_date(timeframe)
    tuds = (purchase.filter(purchase.keen.timestamp >= timeframe_is)
            .filter(purchase.search_info.partner_id == partner)
            .groupBy('flight.time_until_departure')
            # .groupBy('flight.outbound.time_until_departure_int')
            .sum('purchase.total_amount').collect())
    for row in tuds:
        for i in range(len(keys) - 1):
            if row[0] > keys[i] and row[0] <= keys[i+1]:
                result[keys[i+1]] += row[1]
    result.pop(0)
    return result


def get_tud_loss(timeframe, partner, claim):
    """
    returns the distribution of losses by time until departure for a partner over a time period ending today
    input: a period (string), a partner (string), a spark dataframe
    output: a dictionary with the tud bucket and the values associated
    """
    keys = [0, 21, 28, 35, 42, 49, 56, 200] # the predefined bucket less than 3 weeks, btw 3 and 4 weeks, ...
    result = collections.OrderedDict()
    result = {key: 0.0 for key in keys}
    timeframe_is = get_date(timeframe)
    tuds = (claim.filter(claim.keen.timestamp >= timeframe_is)
            .filter(claim.search_info.partner_id == partner)
            .groupBy('flight.outbound.time_until_departure_int')
            .sum('claim.payout_total').collect())
    for row in tuds:
        for i in range(len(keys) - 1):
            if row[0] > keys[i] and row[0] <= keys[i+1]:
                result[keys[i+1]] += row[1]
    result.pop(0)
    return result


def get_farekeep_distrib(timeframe, partner, purchase):
    """
    returns the distribution of profit by price bucket for a partner over a time period ending today
    input: a period (string), a partner (string), a spark dataframe
    output: a dictionary with the bucket and the values associated
    """
    keys = [0, 5, 10, 15, 20, 25, 30, 50] # the predefined bucket by $5 increments until 30 ...
    result = collections.OrderedDict()
    result = {key: 0 for key in keys}
    timeframe_is = get_date(timeframe)
    prices = (purchase.filter(purchase.keen.timestamp >= timeframe_is)
              .filter(purchase.search_info.partner_id == partner)
              .groupBy('purchase.fare_keep_info.fare_keep_price')
              .sum('purchase.quantity').collect())
    for row in prices:
        for i in range(len(keys) - 1):
            if row[0] > keys[i] and row[0] <= keys[i+1]:
                result[keys[i+1]] += row[1]
    result.pop(0)
    return result


def get_payout_distrib(timeframe, partner, claim):
    """
    returns the distribution of payout by buckets for a partner over a time period ending today
    input: a period (string), a partner (string), a spark dataframe
    output: a dictionary with the bucket and the values associated
    """
    keys = [0, 10, 20, 40, 80, 140, 200] # the predefined bucket ...
    result = collections.OrderedDict()
    result = {key: 0 for key in keys}
    timeframe_is = get_date(timeframe)
    payouts = (claim.filter(claim.keen.timestamp >= timeframe_is)
               .filter(claim.search_info.partner_id == partner)
               .groupBy('claim.payout')
               .sum('claim.quantity').collect())
    for row in payouts:
        for i in range(len(keys) - 1):
            if row[0] > keys[i] and row[0] <= keys[i+1]:
                result[keys[i+1]] += row[1]
    result.pop(0)
    return result


def get_owrt_purchases(timeframe, partner, purchase):
    """
    returns the number of oneway and roundtrip sold by a partner over a time period ending today
    input: a period (string), a partner (string), a spark dataframe
    output: a dictionary with the bucket and the values associated
    """
    result = dict()
    timeframe_is = get_date(timeframe)
    result['round_trip'] = (purchase.filter(purchase.keen.timestamp >= timeframe_is)
                            .filter(purchase.search_info.partner_id == partner)
                            .filter(purchase.flight.inbound.departure_datetime != 'NaN')
                            .agg({'purchase.quantity':'sum'}).collect()[0][0])
    result['one_way'] = (purchase.filter(purchase.keen.timestamp >= timeframe_is)
                         .filter(purchase.search_info.partner_id == partner)
                         .filter(purchase.flight.inbound.departure_datetime == 'NaN')
                         .agg({'purchase.quantity':'sum'}).collect()[0][0])
    return result


def get_owrt_searches(timeframe, partner, premium):
    """
    returns the number of oneway and roundtrip searched by a partner over a time period ending today
    input: a period (string), a partner (string), a spark dataframe
    output: a dictionary with the bucket and the values associated
    """
    result = dict()
    timeframe_is = get_date(timeframe)
    result['round_trip'] = (premium.filter(premium.keen.timestamp >= timeframe_is)
                            .filter(premium.search_info.partner_id == partner)
                            .filter(premium.flight.is_one_way == False)
                            .agg({'flight.ticket_quantity':'sum'}).collect()[0][0])
    result['one_way'] = (premium.filter(premium.keen.timestamp >= timeframe_is)
                         .filter(premium.search_info.partner_id == partner)
                         .filter(premium.flight.is_one_way == True)
                         .agg({'flight.ticket_quantity':'sum'}).collect()[0][0])
    return result


def get_lot_purchases(timeframe, partner, purchase):
    """
    returns the distribution of length of trip for farekeep sold by a partner over a time period ending today
    input: a period (string), a partner (string), a spark dataframe
    output: a dictionary with the bucket and the values associated
    """
    result = {}
    timeframe_is = get_date(timeframe)
    new_df = (purchase.filter(purchase.keen.timestamp >= timeframe_is)
              .filter(purchase.search_info.partner_id == partner)
              .filter(purchase.flight.inbound.departure_datetime != 'NaN')
              .withColumn("length_of_trip", datediff(purchase.flight.inbound.departure_datetime, purchase.flight.outbound.departure_datetime)))
    lots = new_df.groupBy('length_of_trip').sum('purchase.quantity').collect()
    for row in lots:
        result[row[0]] = row[1]
    return result


def get_lot_searches(timeframe, partner, premium):
    """
    returns the distribution of length of trip for farekeep searched by a partner over a time period ending today
    input: a period (string), a partner (string), a spark dataframe
    output: a dictionary with the bucket and the values associated
    """
    result = {}
    timeframe_is = get_date(timeframe)
    # look for datediff in spark (working with string)
    new_df = (premium.filter(premium.keen.timestamp >= timeframe_is)
              .filter(premium.search_info.partner_id == partner)
              .filter(premium.flight.is_one_way == False)
              .withColumn("length_of_trip", datediff(premium.flight.return_date, premium.flight.departure_date)))
    lots = new_df.groupBy('length_of_trip').sum('flight.ticket_quantity').collect()
    for row in lots:
        result[row[0]] = row[1]
    return result


def get_flight_price_quotes(timeframe, partner, quote):
    """
    returns the distribution of prices for underlying flights for farekeep quoted by a partner over a time period ending today
    input: a period (string), a partner (string), a spark dataframe
    output: a dictionary with the bucket and the values associated
    """
    keys = [0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 10000] # the predefined bucket by $5 increments until 30 ...
    result = collections.OrderedDict()
    result = {key: 0 for key in keys}
    timeframe_is = get_date(timeframe)
    prices = (quote.filter(quote.keen.timestamp >= timeframe_is)
              .filter(quote.search_info.partner_id == partner)
              .groupBy('quote.current_fare')
              .sum('flight.quantity').collect())
    for row in prices:
        for i in range(len(keys) - 1):
            if row[0] > keys[i] and row[0] <= keys[i+1]:
                result[keys[i+1]] += row[1]
    result.pop(0)
    return result


def get_flight_price_purchases(timeframe, partner, purchase):
    """
    returns the distribution of prices for underlying flights for farekeep purchased by a partner over a time period ending today
    input: a period (string), a partner (string), a spark dataframe
    output: a dictionary with the bucket and the values associated
    """
    keys = [0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 10000] # the predefined bucket by $5 increments until 30 ...
    result = collections.OrderedDict()
    result = {key: 0 for key in keys}
    timeframe_is = get_date(timeframe)
    prices = (purchase.filter(purchase.keen.timestamp >= timeframe_is)
              .filter(purchase.search_info.partner_id == partner)
              .groupBy('purchase.fare_keep_info.trip_lock_in_price')
              .sum('purchase.quantity').collect())
    for row in prices:
        for i in range(len(keys) - 1):
            if row[0] > keys[i] and row[0] <= keys[i+1]:
                result[keys[i+1]] += row[1]
    result.pop(0)
    return result


def get_average_expected_change(timeframe, partner, purchase):
    """
    returns the average expected change of day 1 to day 7
    input: a period (string), a partner (string), a spark dataframe
    output: a dictionary with key=day and value=average_expected_change
    """
    keys = [1,2,3,4,5,6,7]
    result = {}
    result = {key: 0 for key in keys}
    timeframe_is = get_date(timeframe)
    unlisted = purchase.select(explode(purchase.prediction.days).alias("test")).collect()
    # print 'length of unlisted:', len(unlisted)
    total = len(unlisted) / len(keys)
    # print total
    for i in range(len(unlisted)):
        result[i%7+1] += unlisted[i][0].expected_change / total
    return result


def avg_premium_by_price(timeframe, partner, premium): # done
    """
    returns the distribution of avg premium grouped by fare price
    input: a period (string), a partner (string), a spark dataframe
    output: a dictionary with the bucket and the values associated
    """
    timeframe_is = get_date(timeframe)
    result = {}
    a_udf = udf(lambda x: int(x/10)*10)

    result = {}
    avg_prem_by_fare = (premium.filter(premium.keen.timestamp >= timeframe_is)
                        .filter(premium.search_info.partner_id == partner)
                        .withColumn("newCol",a_udf(premium.flight.max_price))
                        .groupBy("newCol")
                        .agg({"premium.max_premium": "avg","premium.max_premium":"count"}).collect())
    # return avg_prem_by_fare
    for row in avg_prem_by_fare:
        result[int(row[0])] = {'avg_prem': row[0], 'count': row[1]}
    return result
    
def avg_premium_by_percent_of_fare(timeframe, partner, premium): # done
    """
    returns the distribution of avg premium grouped by fare price
    input: a period (string), a partner (string), a spark dataframe
    output: a dictionary with the bucket and the values associated
    """
    timeframe_is = get_date(timeframe)
    result = {}
    a_udf = udf(lambda x: int(x)/100.)

    result = {}
    avg_prem_by_fare = (premium.filter(premium.keen.timestamp >= timeframe_is)
                        .filter(premium.search_info.partner_id == partner)
                        .withColumn("newCol",premium.premium.max_premium * 100 / premium.flight.max_price))
    prems = (avg_prem_by_fare.withColumn("newCol2",a_udf(avg_prem_by_fare.newCol))
             .groupBy("newCol2")
             .agg({"premium.max_premium": "avg", "premium.max_premium": "count"}).collect())
    for row in prems:
        result[float(row[0])] = {'avg_prem': row[0], 'count': row[1]}
    return result
    
def avg_premium_by_percent_of_fare_purchase(timeframe, partner, purchase): # done
    """
    returns the distribution of avg premium grouped by fare price
    input: a period (string), a partner (string), a spark dataframe
    output: a dictionary with the bucket and the values associated
    """
    timeframe_is = get_date(timeframe)
    result = {}
    a_udf = udf(lambda x: int(x)/100.)

    result = {}
    avg_prem_by_fare = (purchase.filter(purchase.keen.timestamp >= timeframe_is)
                        .filter(purchase.search_info.partner_id == partner)
                        .withColumn("newCol",purchase.purchase.fare_keep_info.fare_keep_price * 100 / purchase.flight.price))
    prems = (avg_prem_by_fare.withColumn("newCol2", a_udf(avg_prem_by_fare.newCol))
             .groupBy("newCol2")
             .agg({"purchase.fare_keep_info.fare_keep_price": "avg", "purchase.fare_keep_info.fare_keep_price": "count"}).collect())
    for row in prems:
        result[float(row[0])] = {'avg_prem': row[0], 'count': row[1]}
    return result


def get_time_to_purchase(timeframe, partner, premium, purchase): # todo
    """
    returns the distribution of time it takes a user to achieve a purchase
    input: a period (string), a partner (string), 2 spark dataframes (first and last one in the workflow)
    output: a dictionary with the bucket and the values associated
    """
    keys = [0, 20, 40, 60, 120, 180, 240, 300, 600]
    result = collections.OrderedDict()
    result = {key: 0 for key in keys}
    timeframe_is = get_date(timeframe)
    purchase_renam = (purchase.filter(purchase.keen.timestamp >= timeframe_is)
                      .filter(purchase.search_info.partner_id == partner)
                      .withColumnRenamed('keen', 'keen_purchase')
                      .withColumnRenamed('flight', 'flight_purchase'))
    premium_renam = (premium.filter(premium.keen.timestamp >= timeframe_is)
                     .filter(premium.search_info.partner_id == partner)
                     .withColumnRenamed('keen', 'keen_premium')
                     .withColumnRenamed('flight', 'flight_premium'))
    joined_df = purchase_renam.join(premium_renam, purchase_renam.search_info.search_id == premium_renam.search_info.search_id, 'inner')
    joined_df = joined_df.withColumn("time_to_purchase", (minute(joined_df.keen_purchase.timestamp) - minute(joined_df.keen_premium.timestamp)) * 60 + second(joined_df.keen_purchase.timestamp) - second(joined_df.keen_premium.timestamp))
    times = joined_df.groupBy("time_to_purchase").sum("purchase.quantity").collect()
    for row in times:
        for i in range(len(keys) - 1):
            if row[0] > keys[i] and row[0] <= keys[i+1]:
                result[keys[i+1]] += row[1]
    result.pop(0)
    return result


def main():
    CONF = (SparkConf()
            .setMaster("local[3]")
            .set("spark.executor.memory", "2g")
            .setAppName("My App"))
    SC = SparkContext(conf=CONF)
    SQLCONTEXT = SQLContext(SC)
    TIMEFRAME = 'today'
    PARTNER = 'XXX'
    TOP = 5
    PREMIUM = SQLCONTEXT.read.json("/Users/flo/Desktop/FLYR/TA_launch/S3/get_premiums/")
    QUOTE = SQLCONTEXT.read.json("/Users/flo/Desktop/FLYR/TA_launch/S3/get_quotes/")
    PURCHASE = SQLCONTEXT.read.json("/Users/flo/Desktop/FLYR/TA_launch/S3/get_purchases/")
    SEARCHES = get_number_of_searches(TIMEFRAME, PARTNER, PREMIUM)
    FK_SOLD = get_farekeep_sold(TIMEFRAME, PARTNER, PURCHASE)
    COVER = get_coverage(TIMEFRAME, PARTNER, PREMIUM)
    FUNNEL = get_conversion(TIMEFRAME, PARTNER, PREMIUM, QUOTE, PURCHASE)
    TIME_TO_PURCHASE = get_time_to_purchase(TIMEFRAME, PARTNER, PREMIUM, PURCHASE)
    print 'number of searches: ', SEARCHES
    print 'Number of farekeeps sold: ', FK_SOLD
    print 'Coverage: ', COVER
    print 'Funnel Analysis: ', FUNNEL

if __name__ == "__main__":
    main()
    
    

# """
# Main cli or app entry point
# """

from mylib.lib import (
    extract,
    load_data,
    describe,
    query,
    start_spark,
    end_spark,
)


def main():
    # extract data
    extract()
    # start spark session
    spark = start_spark("DailyShowGuests")
    # load data into dataframe
    df = load_data(spark)
    # example metrics
    describe(df)
    # query
    query(
        spark,
        df,
        "SELECT YEAR, COUNT(*) AS guest_count FROM guests GROUP BY YEAR ORDER BY YEAR",
        "guests",
    )
    # example transform
    example_transform(df)
    # end spark session
    end_spark(spark)


# def main()

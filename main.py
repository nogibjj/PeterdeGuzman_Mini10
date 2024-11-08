from mylib.lib import (
    extract,
    load_data,
    describe,
    query,
    transform_region,
    start_spark,
    end_spark,
)


def main():
    # extract data
    extract()
    # start spark session
    spark = start_spark("PoliceKillings2015")
    # load data into dataframe
    df = load_data(spark)
    # example metrics
    describe(df)
    # query
    query(
        spark,
        df,
        "SELECT state, gender, COUNT(*) AS genderbystate_count FROM PoliceKillings2015 GROUP BY state, gender ORDER BY state, gender",
        "PoliceKillings2015",
    )
    # example transform
    transform_region(df)
    # end spark session
    end_spark(spark)


if __name__ == "__main__":
    main()


def write_to_mysql(df, table_name, config, credentials, mode="overwrite"):
    (
        df.write
        .format("jdbc")
        .option("url", config["mysql"]["url"])
        .option("dbtable", table_name)
        .option("user", credentials["user"])
        .option("password", credentials["password"])
        .option("driver", config["mysql"]["driver"])
        .mode(mode)
        .save()
    )

def createStarTable(sourcePath, colList, tableName, tableType="dim"):
    try:
        logger.info(f"Creating {tableType.upper()}_{tableName}")

        inputHub = os.path.join(sourcePath, "delta", "vault", f"hub_{tableName}")
        inputSat = os.path.join(sourcePath, "delta", "vault", f"sat_{tableName}")
        outPath = os.path.join(sourcePath, "delta", "starSCD", f"{tableType}_{tableName}")
        
        hubDF = spark.read.format("delta").load(inputHub)
        satDF = spark.read.format("delta").load(inputSat)

        joinedDF = hubDF.alias("h").join(satDF.alias("s"), col(f"h.{tableName}_HK") == col(f"s.{tableName}_HK"), "inner") \
                        .select(col(f"h.{tableName}_HK"), *[col(f"s.{c}").alias(c) for c in colList], col(f"s.load_date"))

        hashCols = [col(c).cast("string") for c in colList]
        joinedDF = joinedDF.withColumn("record_HK", sha2(concat_ws("||", *hashCols), 256))

        if tableType == "dim":
            winSpec = Window.partitionBy(f"{tableName}_HK").orderBy(col("load_date").desc())
            joinedDF = joinedDF.withColumn("row_no", row_number().over(winSpec)) \
                               .withColumn("next_record_HK", lead("record_HK").over(winSpec)) \
                               .withColumn("is_new", when((col("record_HK") != col("next_record_HK")) | col("next_record_HK").isNull(), lit(True)).otherwise(lit(False)))

            finalDF = joinedDF.filter("is_new = True")
        else:
            winSpec = Window.partitionBy(f"{tableName}_HK").orderBy(col("load_date").desc())
            finalDF = joinedDF.withColumn("row_no", row_number().over(winSpec)).filter(col("row_no") == 1)

        logger.info(f"Total records after join: {joinedDF.count()}, new flagged records: {finalDF.count()}")

        finalDF = finalDF.withColumn(f"surrogate_key_{tableName}", monotonically_increasing_id())

        if tableType == "dim":
            finalDF = finalDF.withColumn("currentVer", when(col("row_no") == 1, lit(1)).otherwise(lit(0))) \
                             .withColumn("activeSince", col("load_date")) \
                             .withColumn("expiryDate", lit("9999-12-31").cast("date"))

        if finalDF.rdd.isEmpty():
            logger.info(f"No new records to insert for {tableType.upper()}_{tableName}")
            return

        if DeltaTable.isDeltaTable(spark, outPath):
            delta_table = DeltaTable.forPath(spark, outPath)
            logger.info(f"Merging into existing Delta table at {outPath}")

            if tableType == "dim":
                condition = f"target.{tableName}_HK = source.{tableName}_HK AND target.currentVer = 1"
                delta_table.alias("target").merge(
                    finalDF.alias("source"),
                    condition=expr(condition)
                ).whenMatchedUpdate(
                    condition="target.record_HK <> source.record_HK",
                    set={
                        "currentVer": lit(0),
                        "expiryDate": expr("current_date()")
                    }
                ).whenNotMatchedInsertAll().execute()
            else:
                condition = "target.record_HK = source.record_HK"
                delta_table.alias("target").merge(
                    finalDF.alias("source"),
                    condition
                ).whenNotMatchedInsertAll().execute()

            logger.debug(f"{tableType.upper()}_{tableName} successfully updated/merged!")
        else:
            logger.info(f"Creating new Delta {tableType.upper()} table at {outPath}")
            finalDF.write.format("delta").save(outPath)
            logger.debug(f"{tableType.upper()}_{tableName} created successfully!")

    except Exception as e:
        logger.error(f"{tableType.upper()}_{tableName} creation failed...", exc_info=True)
        raise
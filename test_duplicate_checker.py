from spark_duplicate_checker import SparkDuplicateChecker

if __name__ == "__main__":
    duplicate_checker = SparkDuplicateChecker()
    
    data = [
        ['A', 'a', 'x', 1],
        ['A', 'b', 'x', 1],
        ['A', 'c', 'x', 1],
        ['B', 'a', 'x', 1],
        ['B', 'b', 'x', 1],
        ['B', 'c', 'x', 1],
        ['A', 'a', 'y', 1],
    ]
    schema = ['col_1', 'col_2', 'col_3', 'col_4']
    df_spark = duplicate_checker.spark.createDataFrame(data=data, schema=schema)
    
    columns_to_check = ['col_1', 'col_2']
    
    duplicate_checker.run_duplicate_check(df_spark, columns_to_check)
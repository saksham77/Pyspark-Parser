from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructType
from pyspark.sql.functions import col, explode

def flatten(df):
    """
    Recursively flattens the given DataFrame.
    """
    complex_fields = {field.name: field.dataType for field in df.schema.fields 
                      if isinstance(field.dataType, (ArrayType, StructType))}

    while complex_fields:
        # Process the first complex field
        field_name, field_type = complex_fields.popitem()
        
        if isinstance(field_type, StructType):
            # Flatten struct fields
            expanded = [col(f"{field_name}.{subfield.name}").alias(f"{field_name}_{subfield.name}")
                        for subfield in field_type]
            df = df.select("*", *expanded).drop(field_name)
        
        elif isinstance(field_type, ArrayType):
            # Explode array fields
            df = df.withColumn(field_name, explode(col(field_name)))
        
        # Update complex fields
        complex_fields = {field.name: field.dataType for field in df.schema.fields 
                          if isinstance(field.dataType, (ArrayType, StructType))}
    
    return df

def read_and_flatten_file(spark, file_path, file_format, row_tag=None):
    """
    Reads and flattens a JSON or XML file.
    
    :param spark: Spark session object
    :param file_path: Path to the input file
    :param file_format: Format of the file ('json' or 'xml')
    :param row_tag: Row tag for XML files (ignored for JSON files)
    :return: Flattened DataFrame
    """
    if file_format == 'json':
        df = spark.read.json(file_path,multiLine=True)
    elif file_format == 'xml':
        df = spark.read.format("xml").option("rowTag", row_tag).load(file_path)
    else:
        raise ValueError("Unsupported file format. Use 'json' or 'xml'.")
    
    return flatten(df)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Flatten JSON and XML") \
    .getOrCreate()

# Specify the input file details
input_file_path = "new_sample.json"  # or "path/to/your/input.xml"
file_format = "json"  # or "xml"
row_tag = None  # only used for XML, ignored for JSON

# Read and flatten the file
flattened_df = read_and_flatten_file(spark, input_file_path, file_format, row_tag)

# Show the flattened DataFrame
flattened_df.show()

# Stop the Spark session
spark.stop()

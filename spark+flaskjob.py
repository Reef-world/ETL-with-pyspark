import time
from flask import Flask
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

app = Flask(__name__)

# Initialization
spark = SparkSession.builder \
    .appName("spark job") \
    .getOrCreate()

#connection properties
jdbc_properties = {
    "user": "sharifah",
    "password": "S1234",
    "driver": "com.mysql.cj.jdbc.Driver"
}

src_url = "jdbc:mysql://35.188.41.58:3306/company-db"
dest_url = "jdbc:mysql://34.66.123.181:3306/dest-company-db"

# map tables to dfs
tables_to_dfs = {
    "Departments": "dep_ext",
    "EMP": "emp_ext", #sample table
    "PAY": "pay_ext" #sample table
}

dataframes = {}

# Extract
for table, df_name in tables_to_dfs.items():
    try:
        df = spark.read.jdbc(url=src_url, table=table, properties=jdbc_properties)
        if df.count() > 0:
            dataframes[df_name] = df
        else:
            print(f"DataFrame for table {table} is empty.")
    except Exception as e:
        print(f"Error loading DataFrame for table {table}: {e}")


emp_ext = dataframes.get("emp_ext")
dep_ext = dataframes.get("dep_ext")
pay_ext = dataframes.get("pay_ext")



# Transformation functions

def employees_transformations(df: DataFrame) -> DataFrame:

    # the rules :
    # concat first and last name into full name column
    # standardize format for the phone number
    # reformat the salary amount to specific decimal point

    emp_trans = df.withColumn("full_Name", concat(col("first_name"), lit(" "), col("last_name"))) \
        .withColumn("phone_number", concat(
        lit("+"),
        when(
            regexp_replace(col("phone_number"), r'\D', '').startswith("00"),
            regexp_replace(col("phone_number"), r'\D', '').substr(3, 999)
        ).otherwise(regexp_replace(col("phone_number"), r'\D', ''))
    )) \
        .withColumn("sa_amount", format_number(col("sa_amount"), 2)) \
        .select("emp_id", "first_name", "last_name", "full_Name", "dep_id", "sa_amount", "email", "job_title", "phone_number")


    invalid_phone_numbers = emp_trans.filter(~col("phone_number").rlike(r'^\+\d+$')).count() #~ for negating
    invalid_emails = emp_trans.filter(~col("email").rlike(r'^[^\s@]+@[^\s@]+\.[^\s@]+$')).count()


    if invalid_phone_numbers > 0 or invalid_emails > 0:
        raise ValueError("DataFrame contains invalid phone numbers or emails.")

    return emp_trans


def payroll_transformations(df):
    #rule :
    #reformat column's decimal point
    #calculate the total pay for each employee (monthly) by summing base pay + allowance + overtime pay, then add it as a column

    sum_cols = ["base_sa", "allowance", "OT_pay"]

    for col_name in sum_cols:
        df = df.withColumn(col_name, col(col_name).cast("decimal(38,2)"))
        #reformating the decimal point

    sum_expression = ' + '.join([f"cast({col_name} as decimal(38,2))" for col_name in sum_cols])

    pay_trans = df.withColumn("total_pay", expr(sum_expression))


    invalid_total_pay = pay_trans.filter(col("total_pay").isNull() | (col("total_pay") < 0)).count()

    if invalid_total_pay > 0:
        raise ValueError(f"Validation Error: {invalid_total_pay} records have invalid total_pay values (null or negative).")


    return pay_trans



def departments_transformations(df):
    #rule
    #give the dep location city a shortcut
    dep_trans = df.withColumn("dep_location", regexp_replace(col("dep_location"), 'Riyadh', 'RUH'))

    remaining_riyadh_count = dep_trans.filter(col("dep_location") == 'Riyadh').count()

    if remaining_riyadh_count > 0:
        raise ValueError(f"Validation Error: {remaining_riyadh_count} records still have 'Riyadh' in 'dep_location'.")

    return dep_trans




def load_df_to_dest(df: DataFrame, table_name: str, dest_url: str, jdbc_properties: dict, max_retries: int = 3) -> None:
    attempt = 0
    while attempt < max_retries:
        try:

            #overwrite for no dups
            df.write \
                .mode('overwrite') \
                .option('batchsize', "100000") \
                .jdbc(url=dest_url, table=table_name, properties=jdbc_properties)

            print(f"Data successfully loaded into table '{table_name}'.")
            return  # Exit function if successful

        except Exception as e:
            attempt += 1
            print(f"Attempt {attempt} failed for table '{table_name}': {e}")
            if attempt >= max_retries:
                print(f"Failed to load data into table '{table_name}' after {max_retries} attempts.")
                raise
            else:
                print(f"Retrying in 5 seconds...")
                time.sleep(5)


@app.route('/run_spark_job', methods=['GET'])
def run_spark_job():
    try:
        # Apply transformations
        emp_trans = employees_transformations(df=emp_ext)
        dep_trans = departments_transformations(df=dep_ext)
        pay_trans = payroll_transformations(df=pay_ext)

        # Load DataFrames to destination
        load_df_to_dest(dep_trans, "Departments",dest_url,jdbc_properties)
        load_df_to_dest(emp_trans, "Employees",dest_url,jdbc_properties)
        load_df_to_dest(pay_trans, "Payroll",dest_url,jdbc_properties)

        return "Spark job completed successfully!"
    except Exception as e:
        return f"Spark job failed: {e}", 500

if __name__ == "__main__":
    app.run(debug=True)

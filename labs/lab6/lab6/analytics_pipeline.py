import pandas as pd
import matplotlib.pyplot as plt
from prefect import task, flow, get_run_logger

@task
def read_data(file):
    df = pd.read_csv(file)
    print(f"Data shape: {df.shape}")
    return df

@task
def validate_data(df):
    print("Validating data...")
    missing_values = df.isnull().sum()
    print("Missing values:\n", missing_values)
    # For simplicity, drop any rows with missing values
    df_clean = df.dropna()

    return df_clean

@task
def transform_data(df_clean):
    print("Transforming data...")
    # For example, if there is a "sales" column, create a normalized version.
    if "sales" in df_clean.columns:
        df_clean["sales_normalized"] = (df_clean["sales"] - df_clean["sales"].mean()) / df_clean["sales"].std()
    
    return df_clean

@task
def plot_histogram(df_clean):
    if "sales" in df_clean.columns:
        plt.hist(df_clean["sales"], bins=20)
        plt.title("Sales Distribution")
        plt.xlabel("Sales")
        plt.ylabel("Frequency")
        plt.savefig("sales_histogram.png")
        plt.close()
        print("Sales histogram saved to sales_histogram.png")

@task
def run_analytics(df_clean):
    print("Generating analytics report...")
    summary = df_clean.describe()
    summary.to_csv("analytics_summary.csv")
    print("Summary statistics saved to analytics_summary.csv")


@flow(name = 'Analytics_pipeline', log_prints=True)
def run_pipeline():
    print("Reading data...")

    # Assume a dataset with sales figures and other fields is provided.
    df = read_data("analytics_data.csv")
    df_clean = validate_data(df)
    df_clean = transform_data(df_clean)
    run_analytics(df_clean)
    plot_histogram(df_clean)

    print("Analytics pipeline completed.")





    

# def main():
#     # Step 1: Fetch Data
#     print("Reading data...")

#     # Assume a dataset with sales figures and other fields is provided.
#     df = pd.read_csv("data/analytics_data.csv")
#     print(f"Data shape: {df.shape}")

#     # Step 2: Validate Data
#     print("Validating data...")
#     missing_values = df.isnull().sum()
#     print("Missing values:\n", missing_values)
#     # For simplicity, drop any rows with missing values
#     df_clean = df.dropna()

#     # Step 3: Transform Data
#     print("Transforming data...")
#     # For example, if there is a "sales" column, create a normalized version.
#     if "sales" in df_clean.columns:
#         df_clean["sales_normalized"] = (df_clean["sales"] - df_clean["sales"].mean()) / df_clean["sales"].std()

#     # Step 4: Generate Analytics Report
#     print("Generating analytics report...")
#     summary = df_clean.describe()
#     summary.to_csv("data/analytics_summary.csv")
#     print("Summary statistics saved to data/analytics_summary.csv")

#     # Step 5: Create a Histogram for Sales Distribution
#     if "sales" in df_clean.columns:
#         plt.hist(df_clean["sales"], bins=20)
#         plt.title("Sales Distribution")
#         plt.xlabel("Sales")
#         plt.ylabel("Frequency")
#         plt.savefig("data/sales_histogram.png")
#         plt.close()
#         print("Sales histogram saved to data/sales_histogram.png")

#     print("Analytics pipeline completed.")

if __name__ == "__main__":
    run_pipeline()

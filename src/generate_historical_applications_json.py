import json
import random
from faker import Faker
import sys
from pyspark.sql import SparkSession
from pyspark.sql import Row
import os

# Initialize Spark
spark = SparkSession.builder.appName("DataGenerator").getOrCreate()

# Read environment variables
catalog_name = os.getenv('CATALOG_NAME')
schema_name = os.getenv('SCHEMA_NAME')

print(f"Starting script execution with catalog: {catalog_name} and schema: {schema_name}")

faker = Faker()

categories = [
    "Food & Nutrition", "Housing", "Income",
    "Health Care", "Child Care Assistance", "Coping with Crisis"
]

def generate_application():
    category = random.choice(categories)
    assistance_info = {}
    application = {
        "applicationID": faker.uuid4(),
        "dateSubmitted": str(faker.date_this_year()),
        "applicantInfo": {
            "firstName": faker.first_name(),
            "lastName": faker.last_name(),
            "dateOfBirth": str(faker.date_of_birth(minimum_age=18, maximum_age=65)),
            "contactInfo": {
                "phoneNumber": faker.phone_number(),
                "emailAddress": faker.email(),
                "mailingAddress": {
                    "street": faker.street_address(),
                    "city": faker.city(),
                    "state": faker.state(),
                    "zipCode": faker.zipcode()
                }
            }
        },
        "category": category,
    }

    if category == "Food & Nutrition":
        assistance_info = {
            "monthlyIncome": random.randint(1000, 5000),
            "numberOfDependents": random.randint(1, 5),
            "requestedAssistance": random.choice(["SNAP", "WIC", "School Meals"])
        }
    elif category == "Housing":
        assistance_info = {
            "type": random.choice(["Rental Assistance", "Public Housing", "Homelessness Prevention"]),
            "currentLivingSituation": random.choice(["Temporary Shelter", "With Family", "Rented Apartment", "Homeless"]),
            "monthlyIncome": random.randint(1000, 5000),
            "monthlyRent": random.randint(500, 1500),
            "numberOfDependents": random.randint(1, 5)
        }
    elif category == "Income":
        assistance_info = {
            "employmentStatus": random.choice(["Employed", "Unemployed", "Self-Employed"]),
            "monthlyIncome": random.randint(1000, 5000),
            "requestedAssistance": random.choice(["TANF", "EITC", "Job Training"])
        }
    elif category == "Health Care":
        assistance_info = {
            "healthInsuranceStatus": random.choice(["Insured", "Uninsured"]),
            "monthlyIncome": random.randint(1000, 5000),
            "numberOfDependents": random.randint(1, 5),
            "requestedAssistance": random.choice(["Medicaid", "CHIP", "Maternal Health Services"])
        }
    elif category == "Child Care Assistance":
        assistance_info = {
            "employmentStatus": random.choice(["Employed", "In School", "Seeking Employment"]),
            "monthlyIncome": random.randint(1000, 5000),
            "numberOfChildren": random.randint(1, 3),
            "childAgeRanges": random.choices(["Infant", "Toddler", "Preschool", "School-age"], k=3)
        }
    elif category == "Coping with Crisis":
        assistance_info = {
            "typeOfCrisis": random.choice(["Natural Disaster", "Family Emergency", "Domestic Violence", "Homelessness"]),
            "immediateNeeds": random.choice(["Shelter", "Food", "Counseling", "Legal Aid"]),
            "numberOfDependentsAffected": random.randint(1, 5),
            "monthlyIncome": random.randint(0, 5000)
        }

    assistance_key = f"{category.lower().replace(' & ', '').replace(' ', '')}Assistance"
    application[assistance_key] = assistance_info

    return application

if __name__ == '__main__':
    # Generate a list of 2.5 million mock applications
    applications = [generate_application() for _ in range(2500000)]

    # Convert the list to a Spark DataFrame
    applications_df = spark.createDataFrame([Row(**app) for app in applications])

    # Check and create catalog and schema if they don't exist
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

    # Construct the full table path
    table_path = f"{catalog_name}.{schema_name}.bronze_rerouting_application_decisions_dlt"

    # Construct the full table path
    table_path = f"{catalog_name}.{schema_name}.bronze_rerouting_application_decisions_dlt"

    #Overwrite the table (or create a new one if it doesn't exist)
    applications_df.write.format("delta").mode("overwrite").saveAsTable(table_path)

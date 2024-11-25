import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# First I load the data from respective CSV files ##
doctors = pd.read_csv(r'C:\Users\Farouq.Olaniyan\Documents\Farouq_personal\Helium_Health\doctors.csv')
hospital_visits = pd.read_csv(r'C:\Users\Farouq.Olaniyan\Documents\Farouq_personal\Helium_Health\hospital_visits.csv')
patients = pd.read_csv(r'C:\Users\Farouq.Olaniyan\Documents\Farouq_personal\Helium_Health\patients.csv')

# I prefer converting the 'created_at' fields to datetime ##
doctors['created_at'] = pd.to_datetime(doctors['created_at'])
hospital_visits['created_at'] = pd.to_datetime(hospital_visits['created_at'])
patients['created_at'] = pd.to_datetime(patients['created_at'])

# Database Configuration ##
db_config = {
    'user': 'postgres',
    'password': 'Farouq9595',
    'host': 'localhost',
    'port': 5432,
    'database': 'postgres'
}

# Create the schema using psycopg2
schema_name = 'helium_health'

try:
    # Establishing connection with psycopg2
    conn = psycopg2.connect(
        dbname=db_config['database'],
        user=db_config['user'],
        password=db_config['password'],
        host=db_config['host'],
        port=db_config['port']
    )
    conn.autocommit = True  # Ensure immediate commit for schema creation
    cursor = conn.cursor()

    # Create schema if it doesn't exist
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
    print(f"Schema '{schema_name}' has been created or already exists.")

    # Close the connection
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error while creating schema: {e}")

# Use SQLAlchemy for data loading
connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@" \
                    f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
engine = create_engine(connection_string)

# I would then save raw data from the csv files into PostgreSQL in helium health schema ##
doctors.to_sql('doctors', engine, schema=schema_name, if_exists='replace', index=False)
hospital_visits.to_sql('hospital_visits', engine, schema=schema_name, if_exists='replace', index=False)
patients.to_sql('patients', engine, schema=schema_name, if_exists='replace', index=False)

print("Raw data successfully loaded into the PostgreSQL database under the 'helium_health' schema.")

# Merging the datasets for processing ##
merged_data = hospital_visits.merge(doctors, left_on='doctor_id', right_on='id', suffixes=('', '_doctor'))
merged_data = merged_data.merge(patients, left_on='patient_id', right_on='id', suffixes=('', '_patient'))
merged_data['visit_date'] = merged_data['created_at'].dt.date       # Added visit_date column ##
merged_data['month'] = merged_data['created_at'].dt.to_period('M').astype(str)  # Convert Period to string ##

# Saving the merged dataset to CSV ##
final_dataset_path = 'processed_hospital_data.csv'
merged_data.to_csv(final_dataset_path, index=False)

# Saving the processed data to PostgreSQL ##
merged_data.to_sql('processed_hospital_data', engine, schema=schema_name, if_exists='replace', index=False)

# Analysis 1: Number of hospital visits per day ##
visits_per_day = merged_data.groupby('visit_date').size().reset_index(name='num_visits')
visits_per_day.to_sql('visits_per_day', engine, schema=schema_name, if_exists='replace', index=False)

# Analysis 2: Number of patients attended to per doctor per month ##
patients_per_doctor_per_month = (
    merged_data.groupby(['name', 'month'])['patient_id']
    .nunique()
    .reset_index(name='num_patients')
)
patients_per_doctor_per_month.to_sql('patients_per_doctor_per_month', engine, schema=schema_name, if_exists='replace', index=False)

# Analysis 3: Ratio of female to male patient visits per month ##
merged_data['sex'] = merged_data['sex'].str.lower()  # Standardize gender labels ##
gender_ratio_per_month = (
    merged_data.groupby(['month', 'sex']).size()
    .unstack(fill_value=0)
    .reset_index()
    .rename(columns={'female': 'num_female', 'male': 'num_male'})
)
gender_ratio_per_month['female_to_male_ratio'] = (
    gender_ratio_per_month['num_female'] / gender_ratio_per_month['num_male']
)
gender_ratio_per_month.to_sql('gender_ratio_per_month', engine, schema=schema_name, if_exists='replace', index=False)

print("All data and analyses successfully loaded into the PostgreSQL database under the 'helium_health' schema.")

# Save the SQL queries to a text file ##
sql_queries = f"""
1. Number of Hospital Visits per Day:
SELECT visit_date, COUNT(*) AS num_visits
FROM {schema_name}.visits_per_day
GROUP BY visit_date
ORDER BY visit_date;

2. Number of Patients Attended to per Doctor per Month:
SELECT name AS doctor_name, month, num_patients
FROM {schema_name}.patients_per_doctor_per_month
ORDER BY doctor_name, month;

3. Ratio of Female to Male Patient Visits per Month:
SELECT 
    month, 
    num_female,
    num_male,
    female_to_male_ratio
FROM {schema_name}.gender_ratio_per_month
ORDER BY month;
"""

with open('sql_queries.txt', 'w') as file:
    file.write(sql_queries)

print("SQL queries saved to 'sql_queries.txt'.")

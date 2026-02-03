from snowflake.snowpark import Session

connection_parameters = {
    "account": "onnqpxp-ovc21017111.us-east-1",
    "user": "SREETEJA0908",
    "password": "enteryours",
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "USER$SREEenterurs",
    "schema": "ANALYTICS"
}

session = Session.builder.configs(connection_parameters).create()
print("✅ Connected successfully")
session.close()


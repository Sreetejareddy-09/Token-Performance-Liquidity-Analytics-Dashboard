from snowflake.snowpark import Session

connection_parameters = {
    "account": "onnqpxp-ovc21017.us-east-1",
    "user": "SREETEJA0908",
    "password": "Sreetejareddy@0908",
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "USER$SREETEJA0908",
    "schema": "ANALYTICS"
}

session = Session.builder.configs(connection_parameters).create()
print("✅ Connected successfully")
session.close()

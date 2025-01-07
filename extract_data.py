import pandas as pd
from hdbcli import dbapi
import warnings
warnings.filterwarnings("ignore")
import json
import boto3
from botocore.exceptions import ClientError

#get secret credentails from AWS
def get_secret():

    secret_name = "prod/iapa-finance"
    region_name = "eu-west-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

secret = get_secret()

con = dbapi.connect(address=secret["host"],port=secret["port"],user=secret["username"],password=secret["password"])

ns_query =  """SELECT "FiscalYearPeriod" AS "Date","MarketArea","BusinessArea","Commodity","CustomerUnit", "CustomerUnitName",
            "FIREAccount",sum("MISOActualSEK") AS "NetSales"
          FROM "_SYS_BIC"."cdw.Finance.Provisioning.Hadoop/BPCViewForML"(
          placeholder."$$ipFiscalYearPeriodFrom$$" => '202001',
          placeholder."$$ipFiscalYearPeriodTo$$" => '202312',
          placeholder."$$ECBReportingPeriod$$" => '202312')
          WHERE "KPIName" in ('NETSALES')
          and "MarketArea" in ('MANA','MELA','MMEA','MNEA','MOAI')
          and "BusinessArea" in ('BACS','BNEW')
          and "MarketAreaPCodeFlag" = 'Y'
          and "FIREAccount" = '0003012111'
          GROUP BY "FiscalYearPeriod","MarketArea","BusinessArea","Commodity","CustomerUnit", "CustomerUnitName","FIREAccount" """

df_out = pd.read_sql_query(ns_query, con)
df_out.to_csv("ml2025CU.csv",index=False)

print(df_out.shape)


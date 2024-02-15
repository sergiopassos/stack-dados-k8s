# TODO construct readme.md
# TODO build api using fastapi

"""
SQL Server:
- Users
- Credit Card

Postgres:
- Payments
- Subscription
- Vehicle

MongoDB:
- Rides
- Users
- Stripe

Redis:
- Google Auth
- LinkedIn Auth
- Apple Auth
"""

import os
import json
import random
import pandas as pd

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from datetime import datetime
from src.objects import users, rides, payments, vehicle
from src.api import api_requests

load_dotenv()

blob_storage_conn_str = os.getenv("BLOB_STORAGE_CONNECTION_STRING")
container_landing = os.getenv("LANDING_CONTAINER_NAME")

users = users.Users()
rides = rides.Rides()
payments = payments.Payments()
vehicle = vehicle.Vehicle()

api = api_requests.Requests()


class BlobStorage(object):
    """
    This class is used to write data into the landing zone
    """

    def __init__(self, str_blob_stg, container_name):
        """
        Initialize the BlobStorage object.

        Args:
            str_blob_stg: The connection string for the blob storage.
            container_name: The name of the container.
        """

        self.blob_storage_conn_str = str_blob_stg
        self.container_landing = container_name

    @staticmethod
    def create_dataframe(dt, ds_type, is_cpf=False):
        """
        Create a dataframe based on the provided data and data source type.

        Args:
            dt: The data to create the dataframe from.
            ds_type: The type of the data source.
            is_cpf: Whether generates a cpf.

        Returns:
            tuple: A tuple containing the JSON-encoded dataframe and the data source type.
        """

        if ds_type == "redis":
            pd_df = pd.DataFrame(dt)
        else:
            pd_df = pd.DataFrame(dt)
            pd_df['user_id'] = api.gen_user_id()
            pd_df['dt_current_timestamp'] = api.gen_timestamp()

            if is_cpf:
                # TODO cpf_list = [api.gen_cpf() for _ in range(len(pd_df))]
                pd_df['cpf'] = is_cpf

        json_data = pd_df.to_json(orient="records").encode('utf-8')
        return json_data, ds_type

    def upload_blob(self, json_data, file_name):
        """
        Upload a blob to the specified container.

        Args:
            json_data: The JSON data to upload.
            file_name: The name of the file to upload.
        """

        blob_service_client = BlobServiceClient.from_connection_string(self.blob_storage_conn_str)
        container_client = blob_service_client.get_container_client(self.container_landing)
        blob_client = container_client.get_blob_client(file_name)
        blob_client.upload_blob(json_data)

    def write_file(self, ds_type: str):
        """
        Write files based on the specified data source type.

        Args:
            ds_type: The type of the data source.
        """

        gen_cpf = api.gen_cpf()

        year, month, day, hour, minute, second = (
            datetime.now().strftime("%Y %m %d %H %M %S").split()
        )

        params = {'size': 100}
        urls = {
            "users": "https://random-data-api.com/api/users/random_user",
            "credit_card": "https://random-data-api.com/api/business_credit_card/random_card",
            "subscription": "https://random-data-api.com/api/subscription/random_subscription",
            "stripe": "https://random-data-api.com/api/stripe/random_stripe",
            "google_auth": "https://random-data-api.com/api/omniauth/google_get",
            "linkedin_auth": "https://random-data-api.com/api/omniauth/linkedin_get",
            "apple_auth": "https://random-data-api.com/api/omniauth/apple_get"
        }

        if ds_type == "mssql":
            dt_users = users.get_multiple_rows(gen_dt_rows=100)
            dt_credit_card = api.api_get_request(url=urls["credit_card"], params=params)

            users_json, ds_type = self.create_dataframe(dt_users, ds_type, is_cpf=gen_cpf)
            credit_card_json, ds_type = self.create_dataframe(dt_credit_card, ds_type)

            file_prefix = "com.owshq.data" + "/" + ds_type
            timestamp = f'{year}_{month}_{day}_{hour}_{minute}_{second}.json'

            user_file_name = file_prefix + "/users" + "/" + timestamp
            self.upload_blob(users_json, user_file_name)

            credit_card_file_name = file_prefix + "/credit_card" + "/" + timestamp
            self.upload_blob(credit_card_json, credit_card_file_name)

            return user_file_name, credit_card_file_name

        elif ds_type == "postgres":
            dt_payments = payments.get_multiple_rows(gen_dt_rows=100)
            dt_subscription = api.api_get_request(url=urls["subscription"], params=params)
            dt_vehicle = vehicle.get_multiple_rows(gen_dt_rows=100)

            payments_json, ds_type = self.create_dataframe(dt_payments, ds_type)
            subscription_json, ds_type = self.create_dataframe(dt_subscription, ds_type)
            dt_vehicle_json, ds_type = self.create_dataframe(dt_vehicle, ds_type)

            file_prefix = "com.owshq.data" + "/" + ds_type
            timestamp = f'{year}_{month}_{day}_{hour}_{minute}_{second}.json'

            payments_file_name = file_prefix + "/payments" + "/" + timestamp
            self.upload_blob(payments_json, payments_file_name)

            subscription_file_name = file_prefix + "/subscription" + "/" + timestamp
            self.upload_blob(subscription_json, subscription_file_name)

            vehicle_file_name = file_prefix + "/vehicle" + "/" + timestamp
            self.upload_blob(dt_vehicle_json, vehicle_file_name)

            return payments_file_name, subscription_file_name, vehicle_file_name

        elif ds_type == "mongodb":

            dt_rides = rides.get_multiple_rows(gen_dt_rows=100)
            dt_users = api.api_get_request(url=urls["users"], params=params)
            dt_stripe = api.api_get_request(url=urls["stripe"], params=params)

            rides_json, ds_type = self.create_dataframe(dt_rides, ds_type, is_cpf=gen_cpf)
            users_json, ds_type = self.create_dataframe(dt_users, ds_type, is_cpf=gen_cpf)
            stripe_json, ds_type = self.create_dataframe(dt_stripe, ds_type)

            file_prefix = "com.owshq.data" + "/" + ds_type
            timestamp = f'{year}_{month}_{day}_{hour}_{minute}_{second}.json'

            rides_file_name = file_prefix + "/rides" + "/" + timestamp
            self.upload_blob(rides_json, rides_file_name)

            users_file_name = file_prefix + "/users" + "/" + timestamp
            self.upload_blob(users_json, users_file_name)

            stripe_file_name = file_prefix + "/stripe" + "/" + timestamp
            self.upload_blob(stripe_json, stripe_file_name)

            return rides_file_name, users_file_name, stripe_file_name

        elif ds_type == "redis":
            user_id = random.randint(1, 10000)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            dt_google_auth = api.api_get_request(url=urls["google_auth"], params=params)
            dt_google_auth["user_id"] = user_id
            dt_google_auth["timestamp"] = timestamp

            dt_linkedin_auth = api.api_get_request(url=urls["linkedin_auth"], params=params)
            dt_linkedin_auth["user_id"] = user_id
            dt_linkedin_auth["timestamp"] = timestamp

            dt_apple_auth = api.api_get_request(url=urls["apple_auth"], params=params)
            dt_apple_auth["user_id"] = user_id
            dt_apple_auth["timestamp"] = timestamp

            google_auth_json = json.dumps(dt_google_auth, ensure_ascii=False).encode('utf-8')
            linkedin_auth_json = json.dumps(dt_linkedin_auth, ensure_ascii=False).encode('utf-8')
            apple_auth_json = json.dumps(dt_apple_auth, ensure_ascii=False).encode('utf-8')

            file_prefix = "com.owshq.data" + "/" + ds_type
            timestamp = f'{year}_{month}_{day}_{hour}_{minute}_{second}.json'

            google_auth_file_name = file_prefix + "/google_auth" + "/" + timestamp
            self.upload_blob(google_auth_json, google_auth_file_name)

            linkedin_auth_file_name = file_prefix + "/linkedin_auth" + "/" + timestamp
            self.upload_blob(linkedin_auth_json, linkedin_auth_file_name)

            apple_auth_file_name = file_prefix + "/apple_auth" + "/" + timestamp
            self.upload_blob(apple_auth_json, apple_auth_file_name)

            return google_auth_file_name, linkedin_auth_file_name, apple_auth_file_name

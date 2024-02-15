"""
mc alias set orion-minio-dev http://4.153.0.204 data-lake 12620ee6-2162-11ee-be56-0242ac120002
mc ls orion-minio-dev/landing

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

from dotenv import load_dotenv
from io import BytesIO
from minio import Minio
from minio.error import S3Error
from datetime import datetime
from src.api import api_requests
from src.objects import users, rides, payments, vehicle

load_dotenv()

users = users.Users()
rides = rides.Rides()
payments = payments.Payments()
vehicle = vehicle.Vehicle()

api = api_requests.Requests()


class MinioStorage(object):
    """
    This class is used to write data into the MinIO server
    """

    def __init__(self, endpoint=None, access_key=None, secret_key=None, bucket_name=None):
        """
        Initialize the MinioStorage object.

        Args:
            endpoint: The endpoint of the MinIO server.
            access_key: The access key for MinIO.
            secret_key: The secret key for MinIO.
            bucket_name: The bucket name on the MinIO server.
        """
        self.get_config_storage(endpoint, access_key, secret_key, bucket_name)

    def get_config_storage(self, endpoint, access_key, secret_key, bucket_name):
        """
        :param endpoint: The URL of the MinIO server endpoint. If not provided, it will use the value from the environment variable "ENDPOINT".
        :param access_key: The access key for authenticating to the MinIO server. If not provided, it will use the value from the environment variable "ACCESS_KEY".
        :param secret_key: The secret key for authenticating to the MinIO server. If not provided, it will use the value from the environment variable "SECRET_KEY".
        :param bucket_name: The name of the bucket to be accessed on the MinIO server. If not provided, it will use the value from the environment variable "LANDING_BUCKET".
        :return: None

        The method `get_config_storage` initializes the configuration storage by setting up the endpoint, access key, secret key, and bucket name for the MinIO client. It uses the provided values
        * for `endpoint`, `access_key`, `secret_key`, and `bucket_name` parameters, or falls back to the corresponding environment variables if not provided.

        Example usage:
        config_storage = ConfigStorage()
        config_storage.get_config_storage("https://minio.example.com", "access_key123", "secret_key456", "my-bucket")
        """

        endpoint = endpoint or os.getenv("ENDPOINT")
        access_key = access_key or os.getenv("ACCESS_KEY")
        secret_key = secret_key or os.getenv("SECRET_KEY")

        self.bucket_name = bucket_name or os.getenv("LANDING_BUCKET")
        self.client = Minio(endpoint, access_key, secret_key, secure=False)

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

    def upload_data(self, data, object_name):
        """
        Uploads a file to a specified bucket in the MinIO server.

        Args:
            data: Data to be uploaded.
            object_name:  The name of the object.
        """

        try:
            json_buffer = BytesIO(data)

            self.client.put_object(
                self.bucket_name,
                object_name=object_name,
                data=json_buffer,
                length=len(data),
                content_type='application/json'
            )

        except S3Error as exc:
            print(f"error occurred while uploading data, {exc}")

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
            self.upload_data(users_json, user_file_name)

            credit_card_file_name = file_prefix + "/credit_card" + "/" + timestamp
            self.upload_data(credit_card_json, credit_card_file_name)

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
            self.upload_data(payments_json, payments_file_name)

            subscription_file_name = file_prefix + "/subscription" + "/" + timestamp
            self.upload_data(subscription_json, subscription_file_name)

            vehicle_file_name = file_prefix + "/vehicle" + "/" + timestamp
            self.upload_data(dt_vehicle_json, vehicle_file_name)

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
            self.upload_data(rides_json, rides_file_name)

            users_file_name = file_prefix + "/users" + "/" + timestamp
            self.upload_data(users_json, users_file_name)

            stripe_file_name = file_prefix + "/stripe" + "/" + timestamp
            self.upload_data(stripe_json, stripe_file_name)

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
            self.upload_data(google_auth_json, google_auth_file_name)

            linkedin_auth_file_name = file_prefix + "/linkedin_auth" + "/" + timestamp
            self.upload_data(linkedin_auth_json, linkedin_auth_file_name)

            apple_auth_file_name = file_prefix + "/apple_auth" + "/" + timestamp
            self.upload_data(apple_auth_json, apple_auth_file_name)

            return google_auth_file_name, linkedin_auth_file_name, apple_auth_file_name

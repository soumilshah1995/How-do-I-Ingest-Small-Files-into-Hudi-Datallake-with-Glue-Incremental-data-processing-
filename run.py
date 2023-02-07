try:
    import os
    import json
    import sys
    import boto3
    import re
    import datetime
    from datetime import datetime

    from dateutil.parser import parse
    from datetime import datetime, timezone, timedelta

    from dotenv import load_dotenv
    load_dotenv(".env")

except Exception as e:
    print("Error@@@@@@@@@@@ : {} ".format(e))



class AWSS3(object):

    """Helper class to which add functionality on top of boto3 """

    def __init__(self, bucket, aws_access_key_id, aws_secret_access_key, region_name):

        self.BucketName = bucket
        self.client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )


    def put_files(self, Response=None, Key=None):
        """
        Put the File on S3
        :return: Bool
        """
        try:

            response = self.client.put_object(
                ACL="private", Body=Response, Bucket=self.BucketName, Key=Key
            )
            return "ok"
        except Exception as e:
            print("Error : {} ".format(e))
            return "error"

    def item_exists(self, Key):
        """Given key check if the items exists on AWS S3 """
        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return True
        except Exception as e:
            return False

    def get_item(self, Key):

        """Gets the Bytes Data from AWS S3 """

        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return response_new["Body"].read()

        except Exception as e:
            print("Error :{}".format(e))
            return False

    def find_one_update(self, data=None, key=None):

        """
        This checks if Key is on S3 if it is return the data from s3
        else store on s3 and return it
        """

        flag = self.item_exists(Key=key)

        if flag:
            data = self.get_item(Key=key)
            return data

        else:
            self.put_files(Key=key, Response=data)
            return data

    def delete_object(self, Key):

        response = self.client.delete_object(Bucket=self.BucketName, Key=Key,)
        return response

    def get_all_keys(self, Prefix=""):

        """
        :param Prefix: Prefix string
        :return: Keys List
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.BucketName, Prefix=Prefix)

            tmp = []

            for page in pages:
                for obj in page["Contents"]:
                    tmp.append(obj["Key"])

            return tmp
        except Exception as e:
            return []

    def print_tree(self):
        keys = self.get_all_keys()
        for key in keys:
            print(key)
        return None

    def find_one_similar_key(self, searchTerm=""):
        keys = self.get_all_keys()
        return [key for key in keys if re.search(searchTerm, key)]

    def __repr__(self):
        return "AWS S3 Helper class "


import uuid
from faker import Faker

global faker
faker = Faker()

class DataGenerator(object):

    @staticmethod
    def get_data():

        return (
            uuid.uuid4().__str__(),
            faker.name(),
            faker.random_element(elements=('IT', 'HR', 'Sales', 'Marketing')),
            faker.random_element(elements=('CA', 'NY', 'TX', 'FL', 'IL', 'RJ')),
            str(faker.random_int(min=10000, max=150000)),
            str(faker.random_int(min=18, max=60)),
            str(faker.random_int(min=0, max=100000)),
            str(faker.unix_time()),
            faker.email(),
            faker.credit_card_number(card_type='amex')
        )


def main():
    for i in range(0, 20):
        helper = AWSS3(
            aws_access_key_id=os.getenv("DEV_AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("DEV_AWS_SECRET_KEY"),
            region_name=os.getenv("DEV_AWS_REGION_NAME"),
            bucket=os.getenv("S3_BUCKET")
        )
        json_data = DataGenerator.get_data()
        columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts", "email", "credit_card"]
        data = dict(zip(columns, json_data))

        file_name = f'data/{uuid.uuid4().__str__()}.json'
        helper.put_files(Key=file_name, Response=json.dumps(data))
        print(data)


if __name__ == "__main__":
    main()

from airflow.models import Connection
from airflow.utils.db import provide_session
from airflow.hooks.base_hook import BaseHook
from boxsdk import OAuth2
from boxsdk import Client


class BoxHook(BaseHook):
    """
    Wrap around the Box Python SDK
    """
    def __init__(self, box_conn_id):
        self.box_conn_id = self.get_connection(box_conn_id)
        self.access_token = self.box_conn_id.login
        self.refresh_token = self.box_conn_id.schema
        self.CLIENT_ID = self.box_conn_id.extra_dejson.get('client_id')
        self.CLIENT_SECRET = self.box_conn_id.extra_dejson.get('client_secret')

    def get_conn(self):
        @provide_session
        def store_tokens(access_token, refresh_token, session=None):
            (session
                .query(Connection)
                .filter(Connection.conn_id == "{}".format(self.box_conn_id))
                .update({Connection.login: access_token,
                         Connection.schema: refresh_token}))

        oauth = OAuth2(
            client_id=self.CLIENT_ID,
            client_secret=self.CLIENT_SECRET,
            access_token=self.access_token,
            refresh_token=self.refresh_token,
            store_tokens=store_tokens,
        )
        client = Client(oauth)
        return client

    def upload_file(self,
                    folder_id=None,
                    file_path=None,
                    file_name=None,
                    preflight_check=False):
            client = self.get_conn()
            client.folder(folder_id=folder_id).upload(
                                        file_path=file_path,
                                        file_name=file_name,
                                        preflight_check=True)

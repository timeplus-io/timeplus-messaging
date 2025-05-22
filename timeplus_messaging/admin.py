import logging
from proton_driver import client
from proton_driver.errors import Error as ProtonError


class AdminException(Exception):
    """Exception raised by the admin"""

    pass


class TimeplusAdmin:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8463,
        user: str = "default",
        password: str = "",
        database: str = "default",
        **kwargs,
    ):
        self.client = client.Client(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            **kwargs,
        )

        self._logger = logging.getLogger(self.__class__.__name__)

    def create_topic(self, topic: str, partitions: int = 1):
        try:
            # Check if stream exists
            result = self.client.execute(f"SHOW STREAMS LIKE '{topic}'")
            if not result:
                # Default schema for generic messaging
                columns_def = (
                    "_key string DEFAULT '', "
                    "_value string, "
                    "_headers string DEFAULT '{}'"
                )  # _tp_time and _tp_sn are default internal columns, no need create explicitly

                create_sql = f"CREATE STREAM IF NOT EXISTS {topic} ({columns_def}) settings shards={partitions}"
                self.client.execute(create_sql)
                self._logger.info(f"Created stream: {topic}")
            else:
                raise AdminException(f"stream {topic} eixts")

        except ProtonError as e:
            self._logger.error(f"Error ensuring stream exists: {e}")
            raise AdminException(f"Failed to create stream {topic}: {e}")

    def delete_topic(self, topic: str):
        try:
            # Check if stream exists
            result = self.client.execute(f"SHOW STREAMS LIKE '{topic}'")
            if not result:
                raise AdminException(f"stream {topic} not eixts")
            else:
                self.client.execute(f"DROP STREAM {topic}")

        except ProtonError as e:
            self._logger.error(f"Error ensuring stream exists: {e}")
            raise AdminException(f"Failed to create stream {topic}: {e}")

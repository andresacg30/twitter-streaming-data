from spark_streamer.spark_context import start_spark_session

from utils import constants

if __name__ == "__main__":

    start_spark_session(constants.HOST, constants.PORT)

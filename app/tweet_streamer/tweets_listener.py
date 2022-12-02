import socket
import json
import logging
from tweepy.auth import OAuthHandler
from tweepy import Stream
from tweepy import Stream

from utils import constants

class TweetGetter(Stream):
    def __init__(self, client_socket: socket.socket):
        self.client_socket = client_socket
    
    def on_data(self, data):
        try:
            message: dict = json.loads(data)['text'].encode(constants.ENCODING_FORMAT)
            print(message)
            self.client_socket.send(message)
            return True
        except BaseException as e:
            print(f"on_data function: There's an error: {e}")
        return True

    def if_error(self, status):
        print(status)
        return True


class TweetSender(Stream):
    auth = OAuthHandler(constants.API_KEY, constants.API_KEY_SECRET)
    auth.set_access_token(constants.ACCESS_TOKEN, constants.ACCESS_TOKEN_SECRET)

    def __init__(self, client_socket):
        self.client_socket = client_socket

    def send_tweets(self):
        twitter_stream = Stream(self.auth, TweetGetter(self.client_socket))
        twitter_stream.filter(track=constants.HASHTAG)

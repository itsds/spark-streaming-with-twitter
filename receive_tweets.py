import sys
import socket
import tweepy
import json

from tweepy import StreamListener, Stream

class TweetListener(StreamListener):

    def __init__(self, socket_to_connect):
        print("Tweets listener initialized")
        self.socket_to_connect = socket_to_connect

    def on_data(self, data):
        try:
            jsonMessage = json.loads(data)
            message = jsonMessage["text"].encode("utf-8")
            print(message)
            self.socket_to_connect.send(message)

        except BaseException as e:
            print(f'error on data {str(e)}')

        return True

    def on_error(self, status_code):
        print(status_code)
        return True


def connect_to_twitter(connection,track_list):
    consumer_key = ''
    consumer_secret = ''
    access_token = '-'
    access_token_secret = ''

    auth = tweepy.OAuthHandler(consumer_key,consumer_secret)
    auth.set_access_token(access_token,access_token_secret)

    myStreamListener = Stream(auth,TweetListener(connection))
    myStreamListener.filter(track=track_list,languages=["en"])

if __name__ == "__main__":
    if len(sys.argv)<4:
        print("Invalid Input")
        exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    track_list = sys.argv[3]
    con_socket = socket.socket()
    con_socket.bind((host,port))
    print(f'Listening on port : {str(port)}')
    con_socket.listen(5)
    connection,client_address =con_socket.accept()
    print(f'Received requested from : {client_address}')
    print(f'Initializing listener for these tracks : {track_list}')
    connect_to_twitter(connection, track_list)

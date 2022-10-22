from email import message
from pickle import TRUE
import string
from time import sleep
import zmq
import sys
import os

REQUEST_TIMEOUT = 2500
REQUEST_RETRIES = 3
SERVER_ENDPOINT = "tcp://localhost:5559"


def put(socket: zmq.Socket, context, client_id, topic: str, message: str) -> None:
    print("PUT Topic: " + topic + " Message: " + message)

    request_message = f'{client_id} PUT {topic} {message}'.encode('utf-8')

    lazyPirate(socket, context, request_message)

    return

def get(socket: zmq.Socket, context, client_id, topic: str) -> None:
    print("GET Topic: " + topic )

    message_status = load_message_status(client_id, topic)

    request_message = f'{client_id} GET {topic} {message_status}'.encode('utf-8')

    reply = lazyPirate(socket, context, request_message)

    message_id, message_content = parse_get_msg(reply)

    if int(message_id) < 0:
        print(f'ERROR: {message_content}')
        return -1
    
    if message_id != message_status and message_status != '0':
        print(f'ERROR: {message_content}')
        return -1

    update_message_status(client_id, topic, message_id)
    print(f'Message Successfully received with \n\tid = {message_id}\n\tmessage content = {message_content}')

    return 0

def parse_get_msg(message: bytes)-> list:
    message_splited = message.decode("utf-8").split("/", 1)
    
    message_id = message_splited[0]
    message_content = message_splited[1]

    return message_id, message_content


def unsub(socket: zmq.Socket, context, client_id, topic: str) -> None:
    print("UNSUB Topic: " + topic )

    request_message = f'{client_id} UNSUB {topic}'.encode('utf-8')

    lazyPirate(socket, context, request_message)

    return 


def lazyPirate(socket: zmq.Socket, context, request_message):
    socket.send(request_message)
    retries_left = REQUEST_RETRIES
    while True:
        if (socket.poll(REQUEST_TIMEOUT) & zmq.POLLIN) != 0:
            reply = socket.recv()
            print(f'Server Response: {reply}')
            retries_left = REQUEST_RETRIES
            break
        
        retries_left -=1
        print("No Response from Server")
        socket.setsockopt(zmq.LINGER, 0)
        socket.close()
        if retries_left == 0:
            print("Server Seems to be Offline, abandoning...")
            sys.exit()
        print("Reconnecting to Server...")
        socket = context.socket(zmq.REQ)
        socket.connect(SERVER_ENDPOINT)
        print("Resending (%s)")
        socket.send(request_message)
    return reply

def sub(socket: zmq.Socket, context, client_id, topic: str) -> None:
    print("SUB Topic: " + topic )

    request_message = f'{client_id} SUB {topic}'.encode('utf-8')

    lazyPirate(socket, context, request_message)
        
    return 

def load_message_status(client_id, topic):
    filename = f'status/status_{client_id}/{topic}.txt'
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    try:
        with open(filename, "r+") as f:
            return f.read()
    except:
        with open(filename, "w+") as f:
            f.writelines("0")
            return "0"

def update_message_status(client_id, topic, message_id):
    filename = f'status/status_{client_id}/{topic}.txt'
    f =  open(filename, "w+") 
    next_message_id = str(int(message_id) + 1)
    f.write(next_message_id)
    f.close()
    return message_id

def main():
    args = sys.argv[1:]

    if (len(args) < 1):
        print("Invalid command, please enter client ID")
        return

    client_id = args[0]


    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(SERVER_ENDPOINT)

    socket.send(f'{client_id} Node connecting...'.encode('utf-8'))
    message = socket.recv()
    print(f"reply: {message}")

    while True:
        print("Enter command: ")
        args = input().split(" ")
        command = args[0].lower()
        topic = args[1]

        if (len(args) > 2):
             message = " ".join(args[2:])

        if (command == "put"):
            put(socket , context, client_id, topic , message)
        elif (command == "get"):
            get(socket, context, client_id, topic)
        elif (command == "sub"):
            sub(socket, context, client_id, topic)
        elif (command == "unsub"):
            unsub(socket, context, client_id, topic)
        else:
            print("Invalid command")

main()

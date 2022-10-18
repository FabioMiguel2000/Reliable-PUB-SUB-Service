import zmq
import sys
import os

# def parse_message(msg):
#     return msg[2:-2]

# TODO just send message, and that's all it needs... leave the rest to server
def put(socket: zmq.Socket, client_id, topic: str, message: str) -> None:
    print("PUT Topic: " + topic + " Message: " + message)

    socket.send(f'{client_id} PUT {topic} {message}'.encode('utf-8'))

    message = socket.recv()
    print("Resposta do PUT: " ,message)

    return

def get(socket: zmq.Socket, client_id, topic: str) -> None:
    print("GET Topic: " + topic )

    message_status = load_message_status(client_id, topic)
    print(message_status)

    socket.send(f'{client_id} GET {topic} {message_status}'.encode('utf-8'))
    
    message = socket.recv().decode("utf-8") 

    message_id = message[1:2]

    if message_id == str(int(message_status )+ 1) or message_status == '0':
        increment_message_status(client_id, topic, message_id)
        print(f'Message Successfully received with \nid = {message_id}\nmessage content = {message}')
    else:
        print(message)
    # message_id = message
    
    return 



def unsub(socket: zmq.Socket, client_id, topic: str) -> None:
    print("UNSUB Topic: " + topic )

    socket.send(f'{client_id} UNSUB {topic}'.encode('utf-8'))
    
    message = socket.recv()
    print("Resposta do UNSUB: " ,message)

    return 


def sub(socket: zmq.Socket, client_id, topic: str) -> None:
    print("SUB Topic: " + topic )

    socket.send(f'{client_id} SUB {topic}'.encode('utf-8'))
    
    message = socket.recv()
    print("Resposta do SUB: " ,message)

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

def increment_message_status(client_id, topic, message_id):
    filename = f'status/status_{client_id}/{topic}.txt'
    f =  open(filename, "w+") 
    # status_file = open(f"client_{client_id}.txt", 'w+')
    # print(f'file content = {f.read()}')
    # message_status = str(int(f.read()) +1)

    f.write(message_id)
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
    socket.setsockopt_string(zmq.IDENTITY, client_id) 

    socket.connect("tcp://localhost:5559")

    socket.send(f'{client_id} Node connecting...'.encode('utf-8'))
    message = socket.recv()
    print(f"reply: {message}")


    # PUT msg = 1 PUT TOPIC1 MENSAGEM
    # GET msg = 1 GET TOPIC1
    # SUB msg = 1 SUB TOPIC1
    # UNSUB msg = 1 SUB TOPIC1

    while True:
        print("Enter command: ")
        args = input().split(" ")
        command = args[0].lower()
        topic = args[1]

        #message = socket.recv()
        #print(message)

        if (len(args) > 2):
             message = " ".join(args[2:])


        if (command == "put"):
            put(socket , client_id, topic , message)
        elif (command == "get"):
            get(socket, client_id, topic)
        elif (command == "sub"):
            sub(socket, client_id, topic)
        elif (command == "unsub"):
            unsub(socket, client_id, topic)
        else:
            print("Invalid command")


 
    # TODO: Read and parse user commands from CLI

    # socket.close()
    # context.term()

main()
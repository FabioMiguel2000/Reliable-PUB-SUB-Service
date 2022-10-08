import zmq

# TODO just send message, and that's all it needs... leave the rest to server
def put():
    # TODO
    return

def get():
    # TODO
    
    return 

def unsub():
    # TODO

    return 


def sub():
    # TODO

    return 

def main():
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt_string(zmq.IDENTITY, "1")

    socket.connect("tcp://localhost:5559")

    socket.send(b"Node connecting...")

    
    print("waiting...")
    message = socket.recv()
    print(message)


    # TODO: Read and parse user commands from CLI

    # socket.close()
    # context.term()

main()
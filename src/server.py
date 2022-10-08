import zmq


def put():
    # TODO: check if node is subcribed to topic, if not then ignore and warn node
    # TODO: node is subscribed to this topic, then push message to all node's message queue that are subscribed to topic

    return

def get():
    
    # TODO: check topic is subscribed by node and topic exists;
    # TODO: check there is a message available for topic and node; 
    #       if there is a message then send to node, if not let the node wait or warn node there is no message (???decide which one)
    return 

def unsub():
    # TODO: if topic was not subscribed by this node, ignore message and warn the node
    # TODO: if topic subcribed by this node, then remove node from this topic and update json file

    return 


def sub():
    # TODO: if topic does not exist, then create topic (add topic and update json file) and add node to this topic
    # TODO: if topic exists and node not subscribed to this topic, then add node to this topic
    # TODO: if topic exists and node already subscribed, ignore message and warn the node that it is already subscribed
    return 

def parse_msg():
    # TODO: Parse Message, check which operation: GET, PUT, SUB, UNSUB


    return

def main():
    # exemple code from https://zguide.zeromq.org/docs/chapter2/ in rrbroker (Extended Request-Reply)
    context = zmq.Context()
    socket = context.socket(zmq.ROUTER)
    socket.bind("tcp://*:5559")

    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)

    while True:
        socks = dict(poller.poll())

        if socks.get(socket) == zmq.POLLIN:
            message = socket.recv()
            print(message)
            node = "1"
            msg = "Connection established"
            # TODO: Parse the message information, parse_msg()
            socket.send_multipart([ bytes(node, 'utf-8'), b'', msg.encode('utf-8')])
            print("Connection established")

    # socket.close()
    # context.term()

main()
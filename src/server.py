from sqlite3 import connect
import zmq
import json

import json
import os

path = os.getcwd()
path = path + "/memory"
existPath = os.path.exists(path)


def fileToJson():

    if existPath:
        dir_list = os.listdir(path)
        returnJson = []
        for file in dir_list:
            f = open(path+"/"+file)
            data = json.load(f)
            returnJson.append(data)
        return returnJson
    else:
        return []


topicFile = fileToJson()


def jsonToFile():
    # creating a new directory
    if not existPath:
        os.mkdir("memory")
    # Changing current path to the directory
    os.chdir("memory")

    for topic in topicFile:
        path = topic["topic_name"] + '.json'
        with open(path, 'w+') as outfile:
            outfile.write(json.dumps(topic))

    os.chdir("..")


def put(client_id, topic_name, message, socket):
    # TODO: check if node is subcribed to topic, if not then ignore and warn node
    # TODO: node is subscribed to this topic, then push message to all node's message queue that are subscribed to topic

    print(message)
        

    #mensagem não é enviada para o client
    msg = "Put command successfully concluded"
    socket.send_multipart(
        [bytes(client_id, 'utf-8'), b'', msg.encode('utf-8')])

    return


def get(client_id, topic_name, socket):

    # TODO: check topic is subscribed by node and topic exists;
    # TODO: check there is a message available for topic and node;
    #       if there is a message then send to node, if not let the node wait or warn node there is no message (???decide which one)

    msg = ""
    socket.send_multipart(
        [bytes(client_id, 'utf-8'), b'', msg.encode('utf-8')])

    return


def unsub(client_id, topic_name, socket):
    # TODO: if topic was not subscribed by this node, ignore message and warn the node
    # TODO: if topic subcribed by this node, then remove node from this topic and update json file

    msg = "Unsubscribe command successfully concluded"
    socket.send_multipart(
        [bytes(client_id, 'utf-8'), b'', msg.encode('utf-8')])

    return

def topicIndex(topic_name):
    index=-1
    for topic in topicFile:
        index = index+1
        if topic["topic_name"]==topic_name:
            return index
        
    return index

def sub(client_id, topic_name, socket):
    # [x] TODO: if topic does not exist, then create topic (add topic and update json file) and add node to this topic
    # [x] TODO: if topic exists and node not subscribed to this topic, then add node to this topic
    # [x] TODO: if topic exists and node already subscribed, ignore message and warn the node that it is already subscribed

    print(topic_name)

    addFlag = True
    indexTopic = topicIndex(topic_name)
    if indexTopic >= 0:
        newSub = {"subscriber_id": client_id, "messages_queue": []}
        
        for subscriber in topicFile[indexTopic]["subscribers"]:
            if subscriber["subscriber_id"]==client_id: 
                addFlag=False

        if addFlag : topicFile[indexTopic]["subscribers"].append(newSub)

    else:
        newTopic = {"topic_name": topic_name,
        "subscribers": [{"subscriber_id": client_id, "messages_queue": []}],
        "messages": []}
        topicFile.append(newTopic)

    if addFlag: 
        jsonToFile()
        msg = "Subscribe command successfully concluded"
    else:
        msg = "You already subscribed in this topic" 

    print(msg)
    
    socket.send_multipart(
        [bytes(client_id, 'utf-8'), b'', msg.encode('utf-8')])

    return


def parse_msg(socket, message):
    # TODO: Parse Message, check which operation: GET, PUT, SUB, UNSUB, format: <nodeid> <command> <topic_name> [message]
    # PUT msg = 1 PUT TOPIC1 MENSAGEM
    # GET msg = 1 GET TOPIC1
    # SUB msg = 1 SUB TOPIC1
    # UNSUB msg = 1 SUB TOPIC1
    message = message.decode('utf-8')
    tokens = message.split(" ")
    
    if(len(tokens)<3):
        return
    
    client_id = tokens[0]
    operation = tokens[1]
    topic_name = tokens[2]

    if operation == "GET":
        get(client_id, topic_name, socket)

    elif operation == "PUT":
        m = ""
        for i in range(3, len(tokens)):
            m = m + " " + tokens[i]
        put(client_id, topic_name, m, socket)

    elif operation == "SUB":
        sub(client_id, topic_name, socket)

    elif operation == "UNSUB":
        unsub(client_id, topic_name, socket)

    # Invalid message
    error_msg = "Invalid message, please send again in formart: <nodeid> <command> <topic_name> [message]"
    socket.send_multipart([bytes(client_id, 'utf-8'), b'', error_msg.encode('utf-8')])
    
    return -1

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
            print("Mensagem recebida :" ,message.decode('utf-8'))

            # TODO: Parse the message information, parse_msg() - message structure maybe = <nodeid> <command> [topic_name]
            parse_msg(socket, message)

            node = "1"
            msg = "Connection established"
            socket.send_multipart(
                [bytes(node, 'utf-8'), b'', msg.encode('utf-8')])

    # socket.close()
    # context.term()


main()

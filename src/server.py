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

def findTopicIndex(topic_name):
    index=0
    for topic in topicFile:
        if topic["topic_name"]==topic_name:
            return index
        index = index+1
        
    return -1

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
    # TODO: check if if not then ignore and warn node
    # TODO: node is subscribed to this topic, then push message to all node's message queue that are subscribed to topic

    print(message)
        

    #mensagem não é enviada para o client
    msg = "Put command successfully concluded"
    socket.send_multipart(
        [bytes(client_id, 'utf-8'), b'', msg.encode('utf-8')])

    return


def get(clientId, topicName, socket):

    # TODO: check topic is subscribed by node and topic exists;
    # TODO: check there is a message available for topic and node;
    #       if there is a message then send to node, if not let the node wait or warn node there is no message (???decide which one)
    # if not os.path.exists(f'{path}/{topic_name}.json'): # Check if topic exists
    #     msg = f'Unable to perform GET operation, topic = {topic_name} does not exist'
    #     socket.send_multipart(
    #         [bytes(client_id, 'utf-8'), b'', msg.encode('utf-8')])


    topicIndex = findTopicIndex(topicName)
    if topicIndex == -1: # Topic does not exist
        msg = f'Unable to perform GET operation, topic = {topicName} does not exist!'
        sendErrorMsg(socket, clientId, msg)
    
    subscribers = topicFile[topicIndex]["subscribers"]

    for subscriber in subscribers:  # Check if subscriber exist
        if subscriber["subscriber_id"]==clientId: 
            messageId = subscriber["messages_id"] + 1  # Don't think we will need this, because the client will send the message id

            messages = topicFile[topicIndex]["messages"]
            for message in messages:    # Check if associated message exists
                if message["mesasge_id"] == messageId:
                    message_content = message["message_content"]
                    sendMsg(socket, clientId, message_content)
                    return 
            msg = f'Unable to perform GET operation, no message was found in topic = {topicName}'
            sendErrorMsg(socket, clientId, msg)
        
    msg = f'Unable to perform GET operation, subscriber is not subscribed to topic = {topicName}!'
    sendErrorMsg(socket, clientId, msg)

    return

def sendErrorMsg(socket, clientId, message):
    msg = f'ERROR: {message}'
    socket.send_multipart(
            [bytes(clientId, 'utf-8'), b'', msg.encode('utf-8')])

def sendMsg(socket, clientId, message):
    socket.send_multipart(
            [bytes(clientId, 'utf-8'), b'', message.encode('utf-8')])




def unsub(client_id, topic_name, socket):
    # TODO: if topic was not subscribed by this node, ignore message and warn the node
    # TODO: if topic subcribed by this node, then remove node from this topic and update json file
    
    found = False
    indexTopic = findTopicIndex(topic_name)
    for subscriber in topicFile[indexTopic]["subscribers"]:
            if subscriber["subscriber_id"]==client_id:
                found=True

    if (found==False):
        msg = "Unsubscribe command successfully concluded"

    msg = "Unsubscribe command successfully concluded"
    socket.send_multipart(
        [bytes(client_id, 'utf-8'), b'', msg.encode('utf-8')])

    return



def sub(client_id, topic_name, socket):
    # [x] TODO: if topic does not exist, then create topic (add topic and update json file) and add node to this topic
    # [x] TODO: if topic exists and node not subscribed to this topic, then add node to this topic
    # [x] TODO: if topic exists and node already subscribed, ignore message and warn the node that it is already subscribed

    addFlag = True
    #indíce do tópico associado
    indexTopic = findTopicIndex(topic_name)
    if indexTopic >= 0:
        newSub = {"subscriber_id": client_id, "messages_id": 0}
        
        #verify if the subscribe is already subscribed 
        for subscriber in topicFile[indexTopic]["subscribers"]:
            if subscriber["subscriber_id"]==client_id: 
                addFlag=False

        if addFlag : topicFile[indexTopic]["subscribers"].append(newSub)

    else:
        newTopic = {"topic_name": topic_name,
        "subscribers": [{"subscriber_id": client_id, "messages_id": 0}],
        "messages": []}
        topicFile.append(newTopic)

    msg = ""
    if addFlag: 
        jsonToFile()
        msg = msg + "Subscribe command successfully concluded"
    else:
        msg = msg + "You already subscribed in this topic" 

    print(msg)
    
    socket.send_multipart(
        [bytes(client_id, 'utf-8'), b'', msg.encode('utf-8')])

    return


def connection(socket, client_id):
    msg = "Connection established"
    socket.send_multipart(
        [bytes(client_id, 'utf-8'), b'', msg.encode('utf-8')])

def parse_msg(socket, message):
    print(message)
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

    elif operation == "Node":
        connection(socket, client_id)

    # Invalid message
    error_msg = "Invalid message, please send again in formart: <nodeid> <command> <topic_name> [message]"
    socket.send_multipart([bytes(client_id, 'utf-8'), b'', error_msg.encode('utf-8')])
    
    return -1

def main():
    # exemple code from https://zguide.zeromq.org/docs/chapter2/ in rrbroker (Extended Request-Reply)
    context = zmq.Context()
    socket = context.socket(zmq.ROUTER)
    socket.setsockopt(zmq.ROUTER_MANDATORY, 1)
    socket.bind("tcp://*:5559")

    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)


    while True:
        socks = dict(poller.poll())

        if socks.get(socket) == zmq.POLLIN:
            message = socket.recv()
            print("Message received :" ,message.decode('utf-8'))

            # TODO: Parse the message information, parse_msg() - message structure maybe = <nodeid> <command> [topic_name]
            parse_msg(socket, message)

    # socket.close()
    # context.term()


main()

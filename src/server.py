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


def put(clientId, topicName, message, socket):
    topicIndex = findTopicIndex(topicName)
    if topicIndex == -1: # Topic does not exist
        msg = f'Unable to perform PUT operation, topic = {topicName} does not exist!'
        sendErrorMsg(socket, clientId, msg)
        return -1

    messages = topicFile[topicIndex]["messages"]

    newMessage = {
     "message_id": 0 if messages == [] else messages[-1]["message_id"]+1,
     "message_content": message
    }

    messages.append(newMessage)

    jsonToFile()
        
    msg = f"Put command successfully concluded, message with content = '{message}' was added to topic = {topicName}"
    sendMsg(socket, clientId, msg)
    socket.send_multipart(
        [bytes(clientId, 'utf-8'), b'', msg.encode('utf-8')])


    return


def get(clientId, topicName, socket, client_message_id):
    topicIndex = findTopicIndex(topicName)
    if topicIndex == -1: # Topic does not exist
        msg = f'Unable to perform GET operation, topic = {topicName} does not exist!'
        sendErrorMsg(socket, clientId, msg)
        return -1
    
    messages = topicFile[topicIndex]["messages"]

    if len(messages) == 0: # Message List is empty
        msg = f'Unable to perform GET operation, no message was found in topic = {topicName}'
        sendErrorMsg(socket, clientId, msg)
        return -1
    
    subscribers = topicFile[topicIndex]["subscribers"]
    # print(f'subscribers = {subscribers}')

    # if client_message_id == '0':
    #     messageId = subscriber["messages_id"] + 1  # Don't think we will need this, because the client will send the message id
    #     message_content = f'#{messageId}-{messages[0]["message_content"]}'
    #     sendMsg(socket, clientId, message_content)
    #     print(f"GET command successfully concluded! Message = {message_content} was sent to client = {clientId}")
    #     return 0

    for index, subscriber in enumerate(subscribers):  # Check if subscriber exist
        # print(f'{subscriber["subscriber_id"]} compare {clientId}')
        if subscriber["subscriber_id"] == clientId: 
            

            if client_message_id == '0':
                messageId = subscriber["messages_id"] + 1  # Don't think we will need this, because the client will send the message id
                message_content = f'#{messageId}-{messages[0]["message_content"]}'

                topicFile[topicIndex]["subscribers"][index]["messages_id"] = messageId
                jsonToFile()
                sendMsg(socket, clientId, message_content)
                
                print(f"GET command successfully concluded! Message = {message_content} was sent to client = {clientId}")
                return 0
            
            messages = topicFile[topicIndex]["messages"]

            # If client asks for with id = 0, then return first message on list
            # if client_message_id == '0' and len(messages) > 0:
            #     messageId = messages[0]["messages_id"] + 1
            #     message_content = f'#{messageId}-{messages[0]["message_content"]}'
            #     sendMsg(socket, clientId, message_content)
            #     print(f"GET command successfully concluded! Message = {message_content} was sent to client = {clientId}")
            #     return 0
            
            messageId = int(client_message_id) + 1
            for message in messages:    # Check if associated message exists
                if message["message_id"] == messageId:
                    
                    # Increment message id and save the file, this value will let us know the progress of each client and used for deletion of messages
                    topicFile[topicIndex]["subscribers"][index]["messages_id"] = messageId
                    jsonToFile()
                    
                    message_content = f'#{messageId}-{message["message_content"]}'
                    sendMsg(socket, clientId, message_content)
                    print(f"GET command successfully concluded! Message = {message_content} was sent to client = {clientId}")
                    return 0

            msg = f'Unable to perform GET operation, no message was found in topic = {topicName}'
            sendErrorMsg(socket, clientId, msg)
            return -1
        
    msg = f'Unable to perform GET operation, subscriber is not subscribed to topic = {topicName}!'
    sendErrorMsg(socket, clientId, msg)

    return -1

def sendErrorMsg(socket, clientId, message):
    msg = f'ERROR: {message}'
    socket.send_multipart(
            [bytes(clientId, 'utf-8'), b'', msg.encode('utf-8')])
    print(msg)

def sendMsg(socket, clientId, message):
    socket.send_multipart(
            [bytes(clientId, 'utf-8'), b'', message.encode('utf-8')])
    print(message)



def unsub(client_id, topic_name, socket):
    # TODO: if topic doesn't exist, ignore message and warn the node
    # TODO: if topic was not subscribed by this node, ignore message and warn the node
    # TODO: if topic subcribed by this node, then remove node from this topic and update json file
    
    msg = ""
    removeFlag = False
    indexTopic = findTopicIndex(topic_name)
    if indexTopic >= 0:

        indexSub = 0
        for subscriber in topicFile[indexTopic]["subscribers"]:
            if subscriber["subscriber_id"]==client_id: 
                removeFlag = True
                break
            indexSub = indexSub + 1

        print(indexSub)
        if removeFlag : topicFile[indexTopic]["subscribers"].pop(indexSub)

    else:
        msg = "Topic doesn't exist"
        socket.send_multipart(
            [bytes(client_id, 'utf-8'), b'', msg.encode('utf-8')])
        return


    if removeFlag:
        jsonToFile()
        msg = "Unsubscribe command successfully concluded"

    else:
        msg = "You were not subscribed in " + topic_name

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
        message_status = tokens[3]
        get(client_id, topic_name, socket, message_status)
        return 0

    elif operation == "PUT":
        m = ""
        for i in range(3, len(tokens)):
            if i != 3:
                m += " "
            m += tokens[i]
        put(client_id, topic_name, m, socket)
        return 0

    elif operation == "SUB":
        sub(client_id, topic_name, socket)
        return 0

    elif operation == "UNSUB":
        unsub(client_id, topic_name, socket)
        return 0

    elif operation == "Node":
        connection(socket, client_id)
        return 0

    # Invalid message
    error_msg = "Invalid message, please send again in formart: <nodeid> <command> <topic_name> [message]"
    sendErrorMsg(socket, client_id, error_msg)

    
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

from asyncio import sleep
from sqlite3 import connect
from time import sleep
import zmq
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
        sendMsg(socket, msg)
        return -1

    messages = topicFile[topicIndex]["messages"]

    newMessage = {
     "message_id": 0 if messages == [] else messages[-1]["message_id"]+1,
     "message_content": message
    }

    messages.append(newMessage)

    jsonToFile()
        
    msg = f"Put command successfully concluded, message with content = '{message}' was added to topic = {topicName}"
    sendMsg(socket, msg)

    return

def drop_messages_list(topic):
    topicIndex = findTopicIndex(topic)
    messages = topicFile[topicIndex]["messages"]

    if len(messages) <= 1: # Only drop messages if size of message list is > 1
        print("QUITTTT")
        return 0

    subscribers = topicFile[topicIndex]["subscribers"]

    lowest_message_id = 999999

    for subscriber in subscribers:  # lowest id in all subscribers
        if subscriber["messages_id"] < lowest_message_id:
            lowest_message_id = int(subscriber["messages_id"])
    print(f'lowest id = {lowest_message_id}')

    for message in messages:
        if message["message_id"] <= lowest_message_id:
            topicFile[topicIndex]["messages"].pop(0)

    jsonToFile()
    return 
    

def get(clientId, topicName, socket, client_message_id):
    topicIndex = findTopicIndex(topicName)
    if topicIndex == -1: # Topic does not exist
        msg = f'-1/Unable to perform GET operation, topic = {topicName} does not exist!'
        sendMsg(socket, msg)
        return -1
    
    messages = topicFile[topicIndex]["messages"]

    if len(messages) == 0: # Message List is empty
        msg = f'-1/Unable to perform GET operation, no message was found in topic = {topicName}'
        sendMsg(socket, msg)
        return -1
    
    subscribers = topicFile[topicIndex]["subscribers"]

    for index, subscriber in enumerate(subscribers):  # Check if subscriber exist
        # print(f'{subscriber["subscriber_id"]} compare {clientId}')
        if subscriber["subscriber_id"] == clientId: 
            
            if client_message_id == '0': # If '0' is sent, then send whatever is first on message queue of that specific topic
                messageId = messages[0]["message_id"]
                message_content = f'{messageId}/{messages[0]["message_content"]}'

                topicFile[topicIndex]["subscribers"][index]["messages_id"] = messageId
                jsonToFile()

                drop_messages_list(topicName)

                sendMsg(socket, message_content)
                
                print(f"GET command successfully concluded! Message = {message_content} was sent to client = {clientId}")
                return 0
            
            messageId = int(client_message_id) 
            for message in messages:    # Check if associated message id exists in the queue, if yes then send it 
                if message["message_id"] == messageId:
                    
                    topicFile[topicIndex]["subscribers"][index]["messages_id"] = messageId
                    jsonToFile()
                    
                    drop_messages_list(topicName)

                    message_content = f'{messageId}/{message["message_content"]}'
                    sendMsg(socket, message_content)
                    print(f"GET command successfully concluded! Message = {message_content} was sent to client = {clientId}")
                    return 0

            msg = f'-1/Unable to perform GET operation, no message was found in topic = {topicName}'
            sendMsg(socket, msg)
            return -1
        
    msg = f'-1/Unable to perform GET operation, subscriber is not subscribed to topic = {topicName}!'
    sendMsg(socket, msg)

    return -1


def sendMsg(socket, message):
    socket.send(message.encode('utf-8'))
    print(message)



def unsub(client_id, topic_name, socket):
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
        sendMsg(socket, msg)
        return


    if removeFlag:
        jsonToFile()
        msg = "Unsubscribe command successfully concluded"

    else:
        msg = "You were not subscribed in " + topic_name

    sendMsg(socket, msg)

    return


def sub(client_id, topic_name, socket):
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
    
    sendMsg(socket, msg)

    return


def connection(socket, client_id):
    msg = "Connection established"
    sendMsg(socket, msg)

def parse_msg(socket, message):
    print(message)
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
    sendMsg(socket, error_msg)

    
    return -1

def main():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5559")


    while True:
        message = socket.recv()
        print("Message received: " ,message.decode('utf-8'))
        parse_msg(socket, message)

    # socket.close()
    # context.term()

main()

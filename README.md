# SDLE First Assignment

SDLE First Assignment of group T01G14.

## Installation and prerequisite

1. Install Python3, see [official website](https://www.python.org/downloads/)
2. Install ZeroMQ, see [official website](https://zeromq.org/download/)
3. Clone this repository:
```shell
git clone https://git.fe.up.pt/sdle/2022/t1/g14/proj1.git
```
4. Head inside the this project folder, run the following command to install the necessary libraries: 
```shell
pip install -r requirements.txt
```

## Usage Guide
For this publish-subscribe service:
- The Clients:
    - Are be able to do the following operations:
        - SUB - Subscribes a Topic
        - UNSUB - Unsubscribes a Topic
        - PUT - Publishes a Message on a Topic
        - GET - Retrieves a Message from a Topic
    - Topics are identified by an arbitrary string
    - Topics are created implicitly when a subscriber subscribes to a topic that does not exist yet.
    - All subscriptions are durable, according to the Java Message Service's (Jakarta Messaging) terminology.
    - A topic's subscriber should get all messages put on a topic, as long as it calls GET enough times, until it explicitly unsubscribes the topic
    

- The Server:
    - Receives the operations mentioned above from the clients and reacts to them accordingly

### How to run 

#### Server Side Application

Using the Command Line, for Windows users, inside the `/src` directory:

```shell
python server.py
```

Using the Command Line, for Linux or MacOS users, inside the `/src` directory:

```shell
python3 server.py
```

***Note: Only one server should be running at a time*** 

#### Client Side Application

Using the Command Line, for Windows users, inside the `/src` directory:

```shell
python client.py <CLIENT_ID>
```

Using the Command Line, for Linux or MacOS users, inside the `/src` directory:

```shell
python3 client.py <CLIENT_ID>
```

***Note: More than one client can be initiated, as long as the <CLIENT_ID> are different from each other***

While the Client Application is running, the following operation commands can be sent to the server side:
```shell
SUB <TOPIC_NAME>
```
```shell
UNSUB <TOPIC_NAME>
```
```shell
GET <TOPIC_NAME>
```
```shell
PUT <TOPIC_NAME> <MESSAGE_CONTENT>
```
For example, to PUT a message "Porto is very hot today" to the Topic "weather", enter:
```shell
PUT weather Porto is very hot today
```


Group members:

1. Emanuel Trigo (up201605389@edu.fe.up.pt)
2. Fábio Huang (up201806829@edu.fe.up.pt)
3. Sara Pereira (up202204189@edu.fe.up.pt)
4. Valentina Wu (up201907483@edu.fe.up.pt)

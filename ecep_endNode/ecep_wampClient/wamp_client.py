"""
Edge Computing Embedded Platform
Developed by Abhishek Gurudutt, Chinmayi Divakara
Praveen Prabhakaran, Tejeshwar Chandra Kamaal

This is a wamp client function.
Functions provided:
1. Heartbeat
2. Registration
3. Receive commands from user
4. Send response back to user
"""

from autobahn.twisted.wamp import ApplicationSession, ApplicationSessionFactory
from autobahn.twisted.websocket import WampWebSocketClientFactory
from autobahn.wamp.types import ComponentConfig

from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor
from twisted.internet.endpoints import clientFromString
from twisted.python import log

import threading
import time
import sys

from callContainer_api import callContainer

log.startLogging(sys.stdout)
requestReceived = None


##    WAMP Application Class for Writer Client  ##
class ClientWriter(ApplicationSession):
    @inlineCallbacks
    def onJoin(self, details):
        global requestReceived
        requestReceived = self
        yield log.msg('Writer Connected')


##    WAMP Application Class for Reader Client  ##
class ClientReader(ApplicationSession):
    @inlineCallbacks
    def onJoin(self, details):
        log.msg('Reader Connected')

        # This is to handle commands received from user
        self.topic = self.config.extra['cmd']
        def contcmd(args):
            # DEBUG Message
            print('*******************************************************************')
            log.msg('received', args)
            print('*******************************************************************')
            # handle the commands
            resp = callContainer(args)
            sendTo("com.ecep.deviceResponse", resp)

        try:
            print("###########################################################")
            yield self.subscribe(contcmd, self.topic)
            print ("Subscribed to topic: " + self.topic)
            print("###########################################################")
        except Exception as e:
            print("could not subscribe to topic:" + self.topic + " : " + e)


class wampclient(ApplicationSession):
    def __init__(self, device):
        self._topicRead = None
        self._debug = False
        self._debug_wamp = False
        self._debug_app = False

        self._factoryWriter = None
        self._factoryReader = None

        self._realm = None
        self._url = None

        self._extra = {'cmd': 'com.ecep.'+device+'.cmd'}

    def connect(self, ip, port, realm):
        self._realm = realm
        self._url = 'ws://' + ip + ':' + port + '/ws'
        self._reactor_thread = None

        self._session_factoryWriter = None
        self._session_factoryReader = None

        cfgReader = ComponentConfig(self._realm, self._extra)
        cfgWriter = ComponentConfig(self._realm, self._extra)

        self._session_factoryReader = ApplicationSessionFactory(cfgReader)
        self._session_factoryReader.session = ClientReader

        self._session_factoryWriter = ApplicationSessionFactory(cfgWriter)
        self._session_factoryWriter.session = ClientWriter

        self._factoryReader = WampWebSocketClientFactory(self._session_factoryReader, url=self._url,
                                                         )

        self._factoryWriter = WampWebSocketClientFactory(self._session_factoryWriter, url=self._url,
                                                         )

        self._reactor_thread = threading.Thread(target=reactor.run, args=(False,))
        self._reactor_thread.daemon = True

        endpoint_descriptor = 'tcp:' + ip + ':' + port

        self._clientReader = clientFromString(reactor, endpoint_descriptor)
        self._clientReader.connect(self._factoryReader)

        self._clientWriter = clientFromString(reactor, endpoint_descriptor)
        self._clientWriter.connect(self._factoryWriter)

        self._reactor_thread.start()

        return self


# Function used to publish the data 
def sendTo(topic, data):
    print ("publishing to :" + topic + " and sending data: ")
    if topic != 'com.ecep.cpuInfo':
        print (data)
    global requestReceived

    try:
        requestReceived.publish(topic, data)
    except Exception as e:
        print ("Cannot Publish!! Make sure the correct python file is run!! Error: ", e)


if __name__ == '__main__':
    
    """
    For testing
    """

    ip = "ec2-52-39-130-106.us-west-2.compute.amazonaws.com"
    port = '8096'
    realm = u'realm1'

    device = 'beaglebone'
    server = wampclient(device)
    check = server.connect(ip, port, realm)

    while True:
        time.sleep(5)

"""
Edge Computing Embedded Platform
Developed by Abhishek Gurudutt, Chinmayi Divakara
Praveen Prabhakaran, Tejeshwar Chandra Kamaal

Registers the device with a unique name, periodic 
heartbeat message generation and transmit other 
messages such as logs, etc.
"""

import threading
import time
import sys
from uuid import getnode as get_mac
from wamp_client import *
import callContainer_api as cca
import __init__
from ..ecep_docker import cpu_info

ticks = 20 #seconds
client = None
threadLock = threading.RLock()

def init(device):
    """
    Initialize client side wamp
    """
    global client
    client = wampclient(device)


# decorator for threads
def threaded(func):
    """
    A wrapper to create a thread.
    Takes function as input.
    Returns a handler for thread.
    """
    def func_wrapper(*args, **kwargs):
        thread = threading.Thread(target = func, args = args, kwargs = kwargs)
        thread.setDaemon(True)
        thread.start()
        return thread
    return func_wrapper


def formDeviceName():
    """
    This forms the device name according to the
    device MAC and name of the device
    """
    mac_int = get_mac()
    mac_hex = iter(hex(mac_int)[2:].zfill(12))
    mac_str = ":".join(i + next(mac_hex) for i in mac_hex)
    
    cpuName = cpu_info.getCpuName()
    
    if cpuName == None:
        sys.exit("Cannot get device name!! Try running with 'sudo'!!")
        
    cpuName = cpuName[:10]
    
    deviceName = cpuName + '/' + mac_str
    
    return deviceName
    
    
class periodicTransmit(object):
    """
    This class definitions which handle message
    transmission.
    """
    
    def __init__(self, deviceID):
        self._deviceId = deviceID
        self._topic = None
        self._heartbeatData = {}
        self._containerData = {}
        self._cpuInfo = {}
    
    #device registration and heartbeat
    @threaded
    def heartbeat(self):
        while True:
            self._topic = "com.ecep.heartbeat"
            self._heartbeatData['deviceId'] = self._deviceId
            self._heartbeatData['location'] = cpu_info.getDeviceLocation()
            self._heartbeatData['arch'] = cpu_info.getMachineArchitecture()

            global threadLock
            threadLock.acquire()
            try:
                sendTo(self._topic, self._heartbeatData)
            finally:
                threadLock.release()
            time.sleep(ticks)
            
    #Send container status
    @threaded
    def containerStatus(self):
        while True:
            self._topic = "com.ecep.containerStatus"
            self._containerData['deviceId'] = self._deviceId
            self._containerData['info'] = cca.getContainerList()

            global threadLock
            threadLock.acquire()
            try:
                sendTo(self._topic, self._containerData)
            finally:
                threadLock.release()
            time.sleep(ticks*5) #  100 seconds
            
            
    #Send CPU information
    @threaded
    def cpuInfo(self):
        while True:
            self._topic = "com.ecep.cpuInfo"
            self._cpuInfo['deviceId'] = self._deviceId
            self._cpuInfo['info'] = cpu_info.getCpuInfo()

            global threadLock
            threadLock.acquire()
            try:
                sendTo(self._topic, self._cpuInfo)
            finally:
                threadLock.release()
            time.sleep(ticks*5) # 100 seconds


if __name__ == "__main__":
         
    device = formDeviceName()
    init(device)

    # params for wampserver
    ip = sys.argv[1]
    port = sys.argv[2]
    realm = unicode(sys.argv[3])
    path = sys.argv[4]

    __init__.init(path)
    print(ip, port, realm, path)

    global client
    check = client.connect(ip, port, realm)
    
    
    #wait till the connection is established
    time.sleep(5)
    
    #create an instance
    periodicTransmit_I = periodicTransmit(device)
    
    # Launch thread
    handle_heartbeat = periodicTransmit_I.heartbeat()
    handle_containerStatus = periodicTransmit_I.containerStatus()
    handle_cpuInfo = periodicTransmit_I.cpuInfo()
    
    try:    
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        sys.exit(0)
        
    handle_heartbeat.join()
    handle_containerStatus.join()
    handle_cpuInfo.join()

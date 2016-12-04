"""
Edge Computing Embedded Platform
Developed by Abhishek Gurudutt, Chinmayi Divakara
Praveen Prabhakaran, Tejeshwar Chandra Kamaal
"""

import urllib2
import json
import os
import requests

ip = "ec2-52-39-130-106.us-west-2.compute.amazonaws.com"
local_ip = "192.168.0.131"
port = 9000
download_route = '/download'
upload_route = '/upload'
file_root_path = None

def init_fetcher(root_path):
    global file_root_path
    if file_root_path is None:
        file_root_path = root_path


def get_file(**kwargs):
    """
    To download file from server
    :param kwargs: has to contain username, container name and file name
    :return: local path of the downloaded file
    """
    key = ['username', 'containerName', 'filename']
    print kwargs
    for param in key:
        if param not in kwargs:
            raise ValueError("Missing %s" % param)
	

    url = 'http://' + ip + ':' + str(port) + download_route
    print url

    try:
        data = json.dumps(kwargs)
        headers = {'Content-Type': 'application/json'}
        req = urllib2.Request(url, data, headers)
        response = urllib2.urlopen(req)
        print  file_root_path + kwargs['username'] + "_" + kwargs['containerName'] + '/' + kwargs['filename']

        file_path = file_root_path + kwargs['username'] + "_" + kwargs['containerName'] + '/' + kwargs['filename']
        local_path = file_root_path + kwargs['username']+'_'+kwargs['containerName']
        if not os.path.exists(file_root_path + kwargs['username']+'_'+kwargs['containerName']):
            os.makedirs(file_root_path + kwargs['username']+'_'+kwargs['containerName'])

        print(file_path)
        with open(file_path, 'wb') as output:
            while True:
                chunk = response.read(2048)
                if not chunk:
                    break
                output.write(chunk)
                print "downloading"
                output.flush()
        return file_path
    except Exception as e:
            print(e)

def put_file(**kwargs):
    """
    To upload a file to server
    :param kwargs: contains container name, local path and if file is available or not.
    :return: True or false
    """
    key = ['containerName', 'local_path', 'isFile']

    for param in key:
        if param not in kwargs:
            raise ValueError("Missing %s" % param)

    url = 'http://' + ip + ':' + str(port) + upload_route
    upload_kwargs = {'containerName': kwargs['containerName'].split('_')[1], 'username': kwargs['containerName'].split('_')[0]}

    try:
        file_path = kwargs['local_path']
        if kwargs['isFile'] == True:
            input = {'file' : open(file_path, 'rb')}
        else:
            input = {'file': None}

        requests.post(url, data=upload_kwargs, files=input)

        return kwargs['isFile']

    except Exception as e:
        print(e)


if __name__  == "__main__":

    #data = {'username':'admin','containerName':'pull','filename':'Doc1.docx'}
    #get_file(**data)

    data = {'containerName': 'abhi_test', 'local_path': '/home/abhi/output.log', 'isFile': True}
    put_file(**data)


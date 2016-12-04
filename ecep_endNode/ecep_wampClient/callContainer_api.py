"""
Edge Computing Embedded Platform
Developed by Abhishek Gurudutt, Chinmayi Divakara
Praveen Prabhakaran, Tejeshwar Chandra Kamaal

This is to interface between Wamp client and
container API. 
"""
import threading

from ..ecep_docker import container, addFile_toContainer
import fetcher

threads = {}

# Call appropriate functions according to the commands received from user
def callContainer(data):
    """
    Calls the particular container API.
    """

    global pid_application
    # Form the response packet to be sent back to server
    response = data

    # restructure the received commands according to the API calls
    cmd = {}

    # To create a container
    if data['command'] == 'create':
        cmd['name'] = data['containerName']
        cmd['image'] = data['imageName']
        response['ID'] = container.create_containers(cmd)

        if response['ID'] == None:
            response['status'] = 'Create failed'
        else:
            response['status'] = 'Created'

    # To remove a container
    if data['command'] == 'remove':
        cmd['container'] = data['containerName']
        response['success'] = container.delete_container(cmd)

        if response['success']:
            response['status'] = 'Removed'
        else:
            response['status'] = 'Remove failed'

    # To start a container
    if data['command'] == 'start':
        cmd['container'] = data['containerName']
        response['success'] = container.run_container(cmd)

        if response['success']:
            response['status'] = 'Started'
        else:
            response['status'] = 'Start failed'


    # to upload a file and start
    if data['command'] == 'upStart':
        cmd['container'] = data['containerName']
        response['success'] = container.run_container(cmd)

        kwargs = {'username': data['containerName'].split('_')[0], 'containerName':
            data['containerName'].split('_')[1], 'filename': data['filename']}
        cmd['local_path'] = fetcher.get_file(**kwargs)
        
            
        execFile = addFile_toContainer.addFile()
        
        threadId = threading.Thread(target=execFile.copyFileTo_container, kwargs=cmd)
        threadId.setDaemon(True)
        threadId.start()
        
        global threads
        threads[data['containerName']] = threadId

        response['status'] = 'File uploaded and starting'
        

    # To stop a container
    if data['command'] == 'stop':
        cmd['container'] = data['containerName']

        response['success'] = container.stop_container(cmd)

        if response['success']:
            response['status'] = 'Stopped'
        else:
            response['status'] = 'Stop failed'


    # to dowload a log file
    if data['command'] == 'download':
        fetch_kwargs = {'container': data['containerName'], 'container_path': data['container_path']}

        execFile = addFile_toContainer.addFile()
        file = execFile.fetch_results_using_cp(**fetch_kwargs)

        put_kwargs = {'local_path': file, 'containerName': data['containerName']}

        if put_kwargs == None:
            put_kwargs['isFile'] = False
        else:
            put_kwargs['isFile'] = True

        response['success'] = fetcher.put_file(**put_kwargs)

    return response


def getContainerList():
    """
    Gets a list of containers running / created / stopped.
    """
    contList = []
    containerInfo = {}
    args = {'all': 'all'}
    contListRaw = container.list_containers(args)
    
    global threads
    print 'threads: ', threads
    
    #print contListRaw
    for entries in contListRaw:
        containerInfo['status'] = entries['Status']
        containerInfo['containerName'] = entries['Names']
        contList.append(containerInfo.copy())

    return contList

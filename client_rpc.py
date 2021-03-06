import io
import random
import traceback
import logging
import sys
from datetime import datetime
import Pyro4

#
# TCSS 558 - Fall 2014
# Project 3B replica distributed DB
# File: client_rcp.py
# Authors Wiehan Jiang
# Date: 11/04/2014
#

logging.basicConfig(filename='client_rpc.log', level=logging.INFO)
#logging.info('[INFO][%s]:Client Started.' %str(datetime.now()))
data = " ".join(sys.argv[1:])
request = data
#request=raw_input("Command in your request: ").strip()
data = request
random_server = random.randint(1,5)
RPC_Server=Pyro4.Proxy("PYRONAME:server_no%s.tcss558"%random_server)    # use name server object lookup uri shortcut
#print RPC_Server.get_request(request)

try:
    # Communicate with server

    print "[{0}]Sent to server#{2}:{1}".format(str(datetime.now()),data, random_server)
    logging.info("[INFO][{0}]Requesting to server #{2}--> {1}".format(str(datetime.now()),data, random_server))
    received = RPC_Server.get_request(request)
    print "[{0}]Received:{1}".format(str(datetime.now()),received)
    logging.info("[INFO][{0}]Received --> {1}".format(str(datetime.now()),received))
    
except Exception, err:
    print "[ERROR][{0}]Error Communicating with the server. Check log for detailed info!".format(str(datetime.now()),sys.exc_info()[0])
    logging.info("[ERROR][{0}]Error Connecting the server -- {1} Check Server Please!".format(str(datetime.now()),sys.exc_info()[0]))
    logging.info("[ERROR][DETAIL]{0}".format(traceback.format_exc()))

finally:
    exit()


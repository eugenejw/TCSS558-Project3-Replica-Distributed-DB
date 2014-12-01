import io
import prettytable
import texttable as tt
import random
import traceback
import logging
import sys
from datetime import datetime
import Pyro4
import json
import time
import curses

#
# TCSS 558 - Fall 2014
# Project 3B, Replica Distributed DB
# File: client_rcp.py
# Author: Wiehan Jiang
# Date: 11/29/2014
#


class Arbiter(object):

    def __init__(self, privision_pool_number):
      self.data='HB request'
      self.pool_number=privision_pool_number
      self.primary=0
      self.alive_list=[]
      self.down_list=[]
      self.pool_table={}
      self.get_pool_status()
      self.console()


    def console(self):
        curses.wrapper(self.refresh_console)

    def refresh_console(self, window):
        while True:
#            time.sleep(0.7)
            self.get_pool_status()
            temp_list=['|','/','--','\\','|','/','--','\\']
            for each in temp_list:
                window.addstr(10, 0,"Refreshing the Pool Status..." + '[' + each + ']')
                time.sleep(0.1)
                window.refresh()
                window.clear()

            for i in range(10):
                window.addstr(10, 0, "[" + ("=" * i) + ">" + (" " * (10 - i )) + "]Refresh Pool Status in 10S\n" + self.create_table() )
                window.refresh()
                window.clear()
                time.sleep(1)


            window.refresh()
            window.clear()

    def create_table(self):
        with open('pool_table.json', 'r') as f:
            data = json.load(f)
        x = prettytable.PrettyTable(['SERVER', 'SERVER STATUS', 'SERVER ROLE', 'NOTIFICATION', 'WHAT SERVER KNOWS ABOUT ITSELF'])
        new_list = self.alive_list + self.down_list
        for each in new_list:
            x.add_row(['server#%s'%each, data['server#%s'%each][1],data['server#%s'%each][0],data['server#%s'%each][2], 'I am %s'%data['server#%s'%each][3]])
        return str(x)


    def get_pool_status(self):
        #initializing the list
        old_alive_list = self.alive_list 
#        old_down_list = eself.down_list
        self.alive_list = []
        self.down_list=[]
        
        for i in range(self.pool_number):
            if self.send_heartbeat(i+1):
                self.alive_list.append(i+1)
            else:
                self.down_list.append(i+1)
        data = {}
        data['alive_list'] = self.alive_list
        data['down_list'] = self.down_list
        for each in self.alive_list:
            data['server#%s'%each] = ['SECONDARY', "ALIVE", 'NONE', 'nil']
        for each in self.down_list:
            data['server#%s'%each] = ['SECONDARY', 'DOWN', 'NONE', 'nil']
        if self.primary == 0:
            try:
                random_number = random.choice(self.alive_list)
                data['server#%s'%random_number] = ['PRIMARY', 'ALIVE', 'NONE', 'nil']
                self.primary = random_number
                data['PRIMARY'] = self.primary
                with open('pool_table.json', 'w') as f:
                    json.dump(data, f)
            except Exception, err:
                for each in self.down_list:
                    data['server#%s'%each] = ['SECONDARY', 'DOWN', 'WARNING:NO PRIMARY', 'N/A']
                with open('pool_table.json', 'w') as f:
                    json.dump(data, f)
        elif not self.primary == 0 and self.primary in self.alive_list:
            data['server#%s'%self.primary] = ['PRIMARY', 'ALIVE', 'NONE', 'nil']   #remains no change
        elif not self.primary == 0 and self.primary in self.down_list and not self.alive_list == []:
            random_number = random.choice(self.alive_list)
            data['server#%s'%random_number] = ['PRIMARY', 'ALIVE', 'NONE', 'nil']
            self.primary = random_number
            data['PRIMARY'] = self.primary
            with open('pool_table.json', 'w') as f:
                json.dump(data, f)
            for number in self.alive_list:
                self.send_role_update(number)
        elif not self.primary == 0 and self.primary in self.down_list and self.alive_list == []:
            for each in self.down_list:
                data['server#%s'%each] = ['SECONDARY', 'DOWN', 'WARNING:NO PRIMARY', 'nil']
                #when all servers are down
                with open('pool_table.json', 'w') as f:
                    json.dump(data, f)
        if not old_alive_list == self.alive_list: 
            #to inform Primary server to update its self.alive_list
            data['PRIMARY'] = self.primary
            with open('pool_table.json', 'w') as f:
                json.dump(data, f)
            for each in self.alive_list:
                self.send_role_update(each)   


            


        return True

    
    def send_role_update(self, int_number):
      random_server = int_number
      RPC_Server=Pyro4.Proxy("PYRONAME:server_no%s.tcss558"%random_server)    # use name server object lookup uri shortcut
      #print RPC_Server.get_request(request)

      try:
          # Communicate with server

#          print "\n[{0}]HB request Sent to Server#{2}:{1}".format(str(datetime.now()),self.data, random_server)
          logging.info("[INFO][{0}]Role Sync-up Requesting to server #{2}--> {1}".format(str(datetime.now()),self.data, random_server))
          request = self.data
          received = RPC_Server.role_sync_up(request, random_server)
          logging.info("[INFO][{0}]Received --> SERVER knows it is a {1}".format(str(datetime.now()),received))
          return True
    
      except Exception, err:
#          print "[WARNING][{0}]Server#{1} is DOWN!".format(str(datetime.now()),sys.exc_info()[0], random_server)
          logging.info("[WARNING][{0}]Server#{1} is DOWN!".format(str(datetime.now()),sys.exc_info()[0], random_server))
          logging.info("[WARNING][DETAIL]{0}".format(traceback.format_exc()))
          return False

    def send_heartbeat(self, int_number):
      random_server = int_number
      RPC_Server=Pyro4.Proxy("PYRONAME:server_no%s.tcss558"%random_server)    # use name server object lookup uri shortcut
      #print RPC_Server.get_request(request)

      try:
          # Communicate with server

#          print "\n[{0}]HB request Sent to Server#{2}:{1}".format(str(datetime.now()),self.data, random_server)
          logging.info("[INFO][{0}]HB Requesting to server #{2}--> {1}".format(str(datetime.now()),self.data, random_server))
          request = self.data
          received = RPC_Server.heartbeat(request, random_server)
#          print "[{0}]Received:{1}".format(str(datetime.now()),received)
          logging.info("[INFO][{0}]Received --> {1}".format(str(datetime.now()),received))
          return True
    
      except Exception, err:
#          print "[WARNING][{0}]Server#{1} is DOWN!".format(str(datetime.now()),sys.exc_info()[0], random_server)
          logging.info("[WARNING][{0}]Server#{1} is DOWN!".format(str(datetime.now()),sys.exc_info()[0], random_server))
          logging.info("[WARNING][DETAIL]{0}".format(traceback.format_exc()))
          return False
      



if __name__=='__main__':
    privision_pool_number=5
    logging.basicConfig(filename='arbiter.log', level=logging.INFO)
    arbiter_initializer=Arbiter(privision_pool_number)
    

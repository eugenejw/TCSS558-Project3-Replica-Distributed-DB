import pickle
import sys
import re
import curses
import os.path
import logging
import datetime
from datetime import datetime
import Pyro4
import os
import time
import json

#
# TCSS 558 - Fall 2014
# Project 3, rcp multi threaded client/server
# File: client_rcp.py
# Authors Wiehan Jiang
# Date: 11/04/2014
#

class RPC_Server(object):

    def __init__(self, number):
        self.role = ''
        self.local_db_init(number)
        self.number = number
        self.alive_list = []
        self.primary = ''
#        self.pull_mapping_table(number)
#        self.self_check(number)



    def get_role(self):
        try:
            with open('pool_table.json', 'r') as f:
                data = json.load(f)
            print 'DEBUG: %s'%data
            self.primary = data['PRIMARY']

        except Exception, err:
            print 'pool_table does not exist, you should run the arbiter first.'
        if data['server#%s'%self.number][0] == 'PRIMARY':

            self.role = 'PRIMARY'

            if self.alive_list == []:

                self.alive_list = data['alive_list']

                self.primary_periodic_op()   # do the periodic ops only when init or alive_list changed
            else:
                if not self.alive_list == data['alive_list']:
                   self.alive_list = data['alive_list']
                   self.primary_periodic_op()   # do the periodic ops only when init or arbiter's alive_list changed
                   
                else:
                    pass

        else:
            self.role = 'SECONDARY'
        print '[DEBUG] my role is %s'%self.role
        data['server#%s'%str(self.number)][3] = self.role
        with open('pool_table.json', 'w') as f:
              json.dump(data, f)


    def primary_periodic_op(self):

        temp_dic = {}
        for each in self.alive_list:
            if (os.path.isfile('database_no%s.db'%each)):
                
                x = pickle.load(open('database_no%s.db'%each, 'rb'))
                temp_dic = dict(x.items() + temp_dic.items())
                pickle.dump(temp_dic, open('database_no%s.db'%self.number, 'wb'))


    def secondary_periodic_op(self):
        temp_dic = {}
        print 'DEBUG on SErver2: self.primary is %s'%self.primary
        if (os.path.isfile('database_no%s.db'%self.primary)):
           temp_dic = pickle.load(open('database_no%s.db'%self.primary, 'rb'))
           self.db=temp_dic
           print 'DEBUG on SErver2: self.primary is %s'%self.primary
           print 'DEBUG on SErver2: self.db is %s'%self.db
           print 'DEBUG on SErver2: temp_dic is %s'%temp_dic

        pickle.dump(temp_dic, open('database_no%s.db'%self.number, 'wb'))
            

            
        

        

    def heartbeat(self, heart_beat_request, number):
        print "\n[INFO][%s]+++++++++ HB request received from Arbiter+++++++++" %(str(datetime.now()))
        logging.info('[INFO][%s]:+++++++++ HB request recevied from Arbiter ++++++++.' %(str(datetime.now())))

        print "[INFO][%s]+++++++++ HB reponded from server#%s to Arbiter +++++++++" %(str(datetime.now()), number)
        logging.info('[INFO][%s]:+++++++++ HB responded from server#%s to Arbiter ++++++++.' %(str(datetime.now()), number))
        return 'Server#%s is ALIVE with no update.'%self.number

    def role_sync_up(self, role_sync_up_request, number):
        print "\n[INFO][%s]+++++++++ Role Sync-up request received from Arbiter+++++++++" %(str(datetime.now()))
        logging.info('[INFO][%s]:+++++++++ Role Sync-up recevied from Arbiter ++++++++.' %(str(datetime.now())))

        self.get_role()

        print "[INFO][%s]+++++++++ Server#%s now know its role is %s +++++++++" %(str(datetime.now()), number, self.role)
        logging.info('[INFO][%s]:+++++++++ Server#%s now knows its role is %s ++++++++.' %(str(datetime.now()), number, self.role))
        return '%s'%self.role


    def pull_primary_db(self):
        self.secondary_periodic_op()


    def local_db_init(self, number):
        # initiate a distributed DB once server gets running

        if not os.path.isfile('database_no%s.db'%number):
           self.db = {'Jaylene%s'%number: '2533550659', 'Weihan%s'%number: '2065197252'}           
           pickle.dump(self.db, open('database_no%s.db'%number, 'wb'))
        if (os.path.isfile('database_no%s.db'%number)):
           self.db = pickle.load(open('database_no%s.db'%number, 'rb'))
           if self.db == {}:
               self.db = {'Jaylene%s'%number: '2533550659', 'Weihan%s'%number: '2065197252'}           
        print "[INFO][%s]local DB loaded onto Memory on server#%s."%(str(datetime.now()), number)
        logging.info('[INFO][%s]:local DB loaded onto Memory on server#%s.' %(str(datetime.now()), number))


    def _get_data_from_remote_server(self, number, data):
        temp_RPC_Server=Pyro4.Proxy("PYRONAME:server_no%s.tcss558"%number)
        received = temp_RPC_Server.get_request(data)
        return received

    def get_request(self, request):
        print "\n[INFO][%s]+++++++++ new requesting coming in +++++++++" %str(datetime.now())
        logging.info('[INFO][%s]:+++++++++ new requesting coming in ++++++++.' %str(datetime.now()))
        print "[INFO][{0}]Following request coming from Client:".format(str(datetime.now()))
        logging.info('[INFO][{0}]Request coming from Client:'.format(str(datetime.now())))
        # check role first
        if self.role == '':
            self.get_role()
        else:
            pass
        #request detail
        self.data = request
        print "[INFO]Request detail --> \"{0}\"".format(self.data)

        parsed_data = self.input_parser()
        #host is PRIMARY
        if self.role == 'PRIMARY':
            self.get_role()
            print "[INFO]Operation Level --> Locally on Primary Server#%s"%self.number
            sent_back_db_value = self.db_operation(parsed_data)
        #host is SCONDARY, but GET or QUERY is requetsed
        elif self.role == 'SECONDARY' and parsed_data[0] == 'GET':
            print "[INFO]Operation Level --> GET OP, Locally on Secondary Server#%s"%self.number
            sent_back_db_value = self.db_operation(parsed_data)
        #host is SCONDARY, but GET or QUERY is requetsed
        elif self.role == 'SECONDARY' and parsed_data[0] == 'QUERY':
            print "[INFO]Operation Level --> QUERY OP, Locally on Secondary Server#%s"%self.number
            sent_back_db_value = self.db_operation(parsed_data)
        #host is SCONDARY, PUT or DEL is requetsed
        elif self.role == 'SECONDARY' and parsed_data[0] == 'DELETE':
            print "[INFO]Operation Level --> DEL OP, Remotely on Primary Server#%s"%self.primary
                         
            sent_back_db_value = self._get_data_from_remote_server(self.primary, request)
        elif self.role == 'SECONDARY' and parsed_data[0] == 'PUT':
            print "[INFO]Operation Level --> PUT OP, Remotely on Primary Server#%s"%self.primary
            sent_back_db_value = self._get_data_from_remote_server(self.primary, request)
#            sent_back_db_value = self.db_operation(parsed_data)
        else:
            print "Wrong INPUT"
            sent_back_db_value = "Use Command Help to Check."


        print "[INFO][%s]Server responding -->  %s" %(str(datetime.now()),sent_back_db_value)
        logging.info("[INFO][%s]Server responding -->  %s" %(str(datetime.now()),sent_back_db_value))
        print "[INFO][%s]+++++++++ this request session completes ++++++++\n" %str(datetime.now())
        logging.info("[INFO][%s]+++++++++ this request session completes ++++++++"%str(datetime.now()))
        return "{0}".format(sent_back_db_value + '(<-- server#%s)'%self.number)

    def replace_acronym(self,a_dict,check_for,replacement_key,replacement_text):
        c = a_dict.get(check_for)
        if c is not None:
          del a_dict[check_for]
          a_dict[replacement_key] = replacement_text
          self.db = a_dict
        elif c is None:
          a_dict[replacement_key] = replacement_text
          print self.db
        pickle.dump(self.db, open('database_no%s.db'%self.primary, 'wb'))
#        self.self_check(self.number)
        return self.db

    def purge_db(self,a_dict,check_for):
        c = a_dict.get(check_for)
        if c is not None:
          del a_dict[check_for]
          self.db = a_dict
#          pickle.dump(self.db, open('database_no%s.db'%self.number, 'wb')) 
          #update the mapping table
#          self.self_check(self.number)
          pickle.dump(self.db, open('database_no%s.db'%self.primary, 'wb')) 
          return_string = "Key/Value Pair of KEY {%s} has been purged from DB" %(check_for)
          return return_string
        elif c is None:
          return_string = "Sorry, Key/Value Pair of KEY {%s} does not exist in any DB." %(check_for)
          return return_string



    def db_operation(self, input):
        # initiate a DB once server gets running
#        self.db = {'Jaylene': '2533550659', 'Weihan': '2065197252'}
        if re.findall(r"error_input", input[0]):
           return 'Invalid Input, now we support four oprands {QUERY|GET arg1|PUT arg1 arg2|DELETE arg1}'
        if re.findall(r"GET", input[0]):
           self.pull_primary_db()
           if (input[1] in self.db): 
              return_string = "The value of KEY \"%s\" is %s" %(input[1],self.db[input[1]])
              return return_string
           else:
              return_string = "The KEY is not in DB. Try {QUERY} or {PUT} to add one." 
              return return_string
        if re.findall(r"PUT", input[0]):
#           self.db[input[1]] = input[2]
           self.replace_acronym(self.db,input[1],input[1],input[2]) 
           return_string = "Key/Value Pair {%s/%s} has been added to DB" %(input[1],input[2])
           return return_string
        if re.findall(r"QUERY", input[0]):
           self.pull_primary_db()
           if input[1] == 'locally':
               return_string = "local DB (a copy of Primary) status: %s"% self.db
               return return_string
           elif input[1] == 'globally': 
               return_string = "Ditributed Databases status: %s"% self.mapping_table
               return return_string
           else:
               return_string = "Argument(s) not recognizable. Enter \'help\' to check valid argument for QUERY."
               return return_string
        if re.findall(r"DELETE", input[0]):
           return_string=self.purge_db(self.db,input[1]) 

           return return_string


    def input_parser(self):
        #parses the query from client, to check either it is a PUT or GET

        m = re.match(r"(?P<KEY>\w+) (?P<INPUT>.*)", "%s" %self.data)
        if (m.group('KEY') == 'QUERY'):   #query operation
          return ['QUERY', m.group('INPUT')] 
        if (m.group('KEY') == 'PUT'):   # put operation
          m1 = re.match(r"(?P<INPUT1>\w+) (?P<INPUT2>\w+)", m.group('INPUT'))
          return [m.group('KEY'),m1.group('INPUT1'),m1.group('INPUT2')] 
        elif not (m.group('KEY') == 'PUT'): # get and delete operations
          return [m.group('KEY'),m.group('INPUT')]
        else:
          return ['error_input']


if __name__=='__main__':
    server_number = '3'
    logging.basicConfig(filename='server_rpc_no%s.log'%server_number, level=logging.INFO)
    server_initializer=RPC_Server(server_number)
#    logger = logging.getLogger('server_rpc_no1.log')
#    logger.setLevel(logging.DEBUG)
#    logger.basicConfig(filename='server_rpc_no1.log', level=logging.DEBUG, filemode='w')
    logging.basicConfig(filename='server_rpc_no%s.log'%server_number, level=logging.INFO)
    print '[info][%s]:Name space \'server_no%s.tcss558\' assigned to server#%s.' %(str(datetime.now()), server_number, server_number)
    logging.info('[info][%s]:Name space \'server_no%s.tcss558\' assigned to server#%s.' %(str(datetime.now()), server_number, server_number))
    os.system("nohup python -m Pyro4.naming &")
    daemon=Pyro4.Daemon()                 # make a Pyro daemon
    ns=Pyro4.locateNS()                   # find the name server
    uri=daemon.register(server_initializer)   # register the object as a Pyro object
    ns.register("server_no%s.tcss558"%server_number, uri)  # register the object with a name in the name server
    print "[info]Server #%s is Up and Running."%server_number
    logging.info('[%s]:Server #%s Started.' %(str(datetime.now()), server_number))
    daemon.requestLoop()                  # start the event loop of the server to wait for calls



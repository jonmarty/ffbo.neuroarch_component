import sys


from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.logger import Logger

from autobahn.twisted.util import sleep
from autobahn.twisted.wamp import ApplicationSession, ApplicationRunner
from autobahn.wamp.exception import ApplicationError
from autobahn.wamp.types import RegisterOptions

import os
import argparse
import six
import txaio

import time

# Neuroarch Imports
import argparse

import numpy as np
import simplejson as json

from pyorient.ogm import Graph, Config
import pyorient.ogm.graph

from config import *

# Required to handle dill's inability to serialize namedtuple class generator:
setattr(pyorient.ogm.graph, 'orientdb_version',
        pyorient.ogm.graph.ServerVersion)


from neuroarch.models import *
from neuroarch.query import QueryWrapper, QueryString

from autobahn.wamp import auth

# User access
import state

from pyorient.serializations import OrientSerialization

import uuid

from twisted.internet import reactor, threads
from itertools import islice


def byteify(input):
    if isinstance(input, dict):
        return {byteify(key): byteify(value)
                                for key, value in input.iteritems()}
    elif isinstance(input, list):
                return [byteify(element) for element in input]
    elif isinstance(input, unicode):
                return input.encode('utf-8')
    else:
        return input
    
def chunks(data, SIZE=1000):
    it = iter(data)
    for i in xrange(0, len(data), SIZE):
        yield {k:data[k] for k in islice(it, SIZE)}

class neuroarch_server(object):
    """ Methods to process neuroarch json tasks """ 

    def __init__(self,database='/na_server',username='root',password='root', user=None):
        try:
            self.graph = Graph(Config.from_url(database, username, password, initial_drop=False,serialization_type=OrientSerialization.Binary))
        except Exception as e:
            print e
            print "WARNING: Serialisation flag ignored"
            self.graph = Graph(Config.from_url(database, username, password, initial_drop=False))
        self.graph.include(Node.registry)
        self.graph.include(Relationship.registry)
        self.user = user
        self.query_processor = query_processor(self.graph)

    def retrieve_neuron(self,nid):
        # WIP: Currently retrieves all information for the get_as method, this will be refined when we know what data we want to store and pull out here
        try:
            n = self.graph.get_element(nid)
            if n == None:
                return {}
            else:
                output = QueryWrapper.from_objs(self.graph,[n])
                return output.get_as()[0].to_json()
        except Exception as e:
            raise e

    # Hackathon 2017
    def retrieve_by_id(self,task,threshold):
        # Retrieve an object by ID, in order to allow direct addressing to na objects or vfb neurons.
        # WIP: Test thresholding and chunking with larger NA 'tags'

        key_types = ['na','vfb']#,'fc']  # A list of valid ids
                                        # na is a direct neuroarch ID minus the #
                                        # vfb is virtual fly brain with tag vib_id
                                        # fc will be fly circuit, currently in name
        
        if not type(task) == dict:
            task = json.loads(task)
        task = byteify(task)
        
        user = self.user
        
        key_type = task["key_type"]
        key = task["key"]

        assert key_type in key_types
        if key_type == 'na':
            try:
                n = self.graph.get_element('#'+key)
                # WIP: support tags  
            except Exception as e:
                raise e
        elif key_type == 'vfb': 
            n= self.graph.Neurons.query(vfb_id=key).first()
        else:
            pass

        if n == None:
            return ({},False)
        else:
            output = QueryWrapper.from_objs(self.graph,[n])
            # Add hook into user system
            user.append(output)
            
            df = output.get_data(cls='MorphologyData')[0]
            output = df[['sample','identifier','x','y','z','r','parent','name']].to_dict(orient='index')
            
            if threshold and isinstance(output, dict):
                chunked_output = []
                for c in chunks(output, threshold):
                    chunked_output.append(c)
                output = chunked_output
 
            return (output, True) 

    def process_query(self,task):
        """ configure a task processing, and format the results as desired """
        # WIP: Expand type of information that can be retrieved
        assert 'query'in task.keys()
        try:
            self.query_processor.process(task['query'],self.user)
            return True
        except Exception as e:
            print e
            return False

    @staticmethod
    def process_verb(output, user, verb):
        if verb == 'add':
            assert(len(user.state)>=2)
            user.state[-1] = output+user.state[-2]
        elif verb == 'keep':
            assert(len(user.state)>=2)
            user.state[-1] = output & user.state[-2]
            output = user.state[-2] - user.state[-1]
        elif verb == 'remove':
            assert(len(user.state)>=2)
            user.state[-1] = user.state[-2] - output
        else:
            assert(len(user.state)>=2)
            cmd = {'undo':{'states':1}}
            output = output & user.state[-2]
            user.process_command(cmd)
        return output
    
    def receive_task(self,task, threshold=None, query_results=True):
        """ process a task of form
            {'query':...} or {'command': ...}
            update the user states, and query neuroarch
            This is the default access route
        """
        if not type(task) == dict:
            task = json.loads(task)
        task = byteify(task)
        
        if 'format' not in task:
            task['format'] = 'morphology'
        
        assert 'query' in task or 'command' in task

        user = self.user
        if 'command' in task:
            output = user.process_command(task['command'])
            if 'verb' in task and not task['verb'] == 'show':
                try:
                    output = self.process_verb(output, user, task['verb'])
                except Exception as e:
                    print e
                if not task['verb'] == 'add':
                    if task['format'] == 'morphology':
                        output=output.get_data_rids(cls='MorphologyData')
                    else:
                        output = output._records_to_list(output.nodes)
                    return (output, True)
                
            
            if isinstance(output, QueryWrapper):
                #print  output._records_to_list(output.nodes)
                if task['format'] == 'morphology':
                    df = output.get_data(cls='MorphologyData')[0]
                    try:
                        output= df[['sample','identifier','x','y','z','r','parent','name']].to_dict(orient='index')
                    except KeyError:
                        output = {}
                elif task['format'] == 'no_result':
                    output = {}
                elif task['format'] == 'get_data':
                    if 'cls' in task:
                        output = output.get_data(cls=task['cls'])[0].to_dict(orient='index')
                    else:
                        output = output.get_data()[0].to_dict(orient='index')
                elif task['format'] == 'nx':
                    nx_graph = output.get_as('nx')
                    output = {'nodes': nx_graph.node, 'edges': nx_graph.edge}
                elif task['format'] == 'nk':
                    output = output.traverse_owned_by_get_toplevel()
                    for x in output['LPU']:
                        g = output['LPU'][x].get_as('nx')
                        output['LPU'][x] = {'nodes': g.node, 'edges': g.edge}
                    for x in output['Pattern']:
                        g = output['Pattern'][x].get_as('nx')
                        output['Pattern'][x] = {'nodes': g.node, 'edges': g.edge}
            
        
                elif task['format'] == 'df':
                    dfs = output.get_as()
                    output = {}
                    if 'node_cols' in task:
                        output['nodes'] = dfs[0][task['node_cols']].to_dict(orient='index')
                    else:
                        output['nodes'] = dfs[0].to_dict(orient='index')
                    if 'edge_cols' in task:
                        output['edges'] = dfs[1][task['edge_cols']].to_dict(orient='index')
                    else:
                        output['edges'] = dfs[1].to_dict(orient='index')
                elif task['format'] == 'qw':
                    pass
                # Default to nodes and edges df
                else:
                    dfs = output.get_as()
                    output = {'nodes':dfs[0].to_dict(orient='index'),
                              'edges': dfs[1].to_dict(orient='index')}
            else:
                output = str(output)
            if threshold and isinstance(output, dict):
                chunked_output = []
                for c in chunks(output, threshold):
                    chunked_output.append(c)
                output = chunked_output
            return (output, True)
            
        
        elif 'query' in task:
            succ = self.process_query(task)
            if query_results:
                task['command'] = {"retrieve":{"state":0}}
                output = (None,)
                try:
                    output = self.receive_task(task, threshold)
                    if output[0]==None:
                        succ=False
                except Exception as e:
                    print e
                    succ = False
                    
                return (output[0], succ) 
            return succ

class query_processor():

    def __init__(self,graph):
        self.class_list = {}
        self.graph = graph
        self.load_class_list()

    def load_class_list(self):
        # Dynamically build acceptable methods from the registry
        # This could potentially be made stricter with a custom hardcoded subset
        for k in Node.registry:
            try:
                plural = eval(k + ".registry_plural")
                self.class_list[k]=eval("self.graph." + plural)
            except:
                print "Warning:Class %s left out of class list" % k
                e = sys.exc_info()[0]
                print e
        #print self.class_list
    
    def process(self,query_list,user):
        """ take a query of the form
           [{'object':...:,'action...'}] 
        """
        assert type(query_list) is list
        
        task_memory = []
        for q in query_list:
            # Assume each query must act on the previous result, is this valid?
            task_memory.append(self.process_single(q,user,task_memory))
        if 'temp' in query_list[-1] and query_list[-1]['temp']:
            return task_memory[-1]
        output = task_memory[-1]

        user.append(output)
        return output


    def process_single(self,query,user,task_memory):
        """  accetpt a single query object or form
           [{'object':...:,'action...'}] 
        """
        assert 'object' in query and 'action' in query
        assert 'class' in query['object'] or 'state' in query['object'] or 'memory' in query['object']
        '''
        if 'class' in query['object']:
            # Retrieve Class
            class_name = query['object']['class']
            na_object = self.class_list[class_name]
            # convert result to a query wrapper to save
        '''
        if 'state' in query['object']:
            state_num = query['object']['state']

            if type(state_num) is long:
                state_num = int(state_num)

            assert type(state_num) in [int,long]
            na_object = user.retrieve(index = state_num)
        
        elif 'memory' in query['object']:
            assert task_memory is not []
            memory_index = query['object']['memory']

            if type(memory_index) is long:
                memory_index = int(memory_index)

            assert type(memory_index) is int
            assert len(task_memory) > memory_index
            na_object = task_memory[-1-memory_index]

        # Retrieve method
        if 'method' in query['action']: # A class query can only take a method.
            if 'class' in query['object']:
                method_call = query['action']['method']
                assert len(method_call.keys()) == 1
                method_name = method_call.keys()[0]
                
                method_args = method_call[method_name]
                columns = ""
                attrs = [] 
                for k, v in method_args.iteritems():
                    if not(isinstance(v, list)):
                        if isinstance(v, (basestring, numbers.Number)):
                            v = [str(v)]
                    else:
                        # To prevent issues with unicode objects
                        if v and isinstance(v[0],basestring): v = [str(val) for val in v]
                    if len(v) == 1 and isinstance(v[0],(unicode,str)) and len(v[0])>=2 and str(v[0][:2]) == '/r':
                        attrs.append("%s matches '%s'" % (str(k), str(v[0][2:])))
                    else:
                        attrs.append("%s in %s" % (str(k), str(v)))
                attrs = " and ".join(attrs)
                if attrs: attrs = "where " + attrs
                query_str = "select from %s %s" % (str(query['object']['class']),str(attrs))
                query_str = QueryString(query_str,'sql')
                query_result = QueryWrapper(self.graph, query_str)
            else:
                method_call = query['action']['method']


                assert len(method_call.keys()) == 1
                method_name = method_call.keys()[0]
                # check method is valid
                assert method_name in dir(type(na_object))
                # Retrieve arguments
                method_args = byteify(method_call[method_name])
            
                if 'pass_through' in method_args:
                    pass_through = method_args.pop('pass_through')
                    if isinstance(pass_through,list) and pass_through and isinstance(pass_through[0],list):
                        query_result = getattr(na_object, method_name)(*pass_through,**method_args)
                    else:
                        query_result = getattr(na_object, method_name)(pass_through,**method_args)
                else:
                    query_result = getattr(na_object, method_name)(**method_args)
            
        elif 'op' in query['action']:
            method_call = query['action']['op']
            assert len(method_call.keys()) == 1
            method_name = method_call.keys()[0]
            # WIP: Check which operators are supported
            # What if we want to be a op between two past states!
            # retieve past external state or internal memory state
            if 'state' in method_call[method_name]:
                state_num = method_call[method_name]['state']
                assert type(state_num) in [int,long]
                past_object = user.retrieve(index = state_num)
            elif 'memory' in method_call[method_name]:
                assert task_memory is not []
                memory_index = method_call[method_name]['memory']

                if type(memory_index) is long:
                    memory_index = int(memory_index)

                assert type(memory_index) is int
                assert len(task_memory) > memory_index
                past_object = task_memory[-1-memory_index]


            #query_result = getattr(na_object, method_name)(method_args)
            ## INVERSE THIS na and method argis (WHY?)
            query_result = getattr(na_object, method_name)(past_object)
            
        # convert result to a query wrapper to save
        if type(query_result) is not QueryWrapper: 
            output = QueryWrapper.from_objs(self.graph,query_result.all())
        else:
            output = query_result

        return output


class user_list():
    def __init__(self,state_limit=10):
        self.list = {}
        self.state_limit = state_limit
        pass

    def user(self,user_id, database='/na_server',username='root',password='root'):
        if user_id not in self.list:
            st = state.State(user_id)
            self.list[user_id] = {'state': st,
                                  'server': neuroarch_server(user=st)}
        return self.list[user_id]

    def cleanup(self):
        cleansed = []
        for user in self.list:
            x = self.list[user]['state'].memory_management()
            if x:
                cleansed.append(user)
        for user in cleansed:
            del self.list[user]
            
        return cleansed
            

class AppSession(ApplicationSession):

    log = Logger()
    
    def onConnect(self):
        if self.config.extra['auth']:
            self.join(self.config.realm, [u"wampcra"], user)
        else:
            self.join(self.config.realm, [], user)

    def onChallenge(self, challenge):
        if challenge.method == u"wampcra":
            #print("WAMP-CRA challenge received: {}".format(challenge))
            
            if u'salt' in challenge.extra:
                # salted secret
                key = auth.derive_key(secret,
                                      challenge.extra['salt'],
                                      challenge.extra['iterations'],
                                      challenge.extra['keylen'])
            else:
                # plain, unsalted secret
                key = secret
                
            # compute signature for challenge, using the key
            signature = auth.compute_wcs(key, challenge.extra['challenge'])
            
            # return the signature to the router for verification
            return signature

        else:
            raise Exception("Invalid authmethod {}".format(challenge.method))

    def na_query_on_end(self):
        self._current_concurrency -= 1
        self.log.info('na_query() ended ({invocations} invocations, current concurrency {current_concurrency} of max {max_concurrency})', invocations=self._invocations_served, current_concurrency=self._current_concurrency, max_concurrency=self._max_concurrency)

    def retrieve_by_id_on_end(self):
        self._current_concurrency -= 1
        self.log.info('retrieve_by_id() ended ({invocations} invocations, current concurrency {current_concurrency} of max {max_concurrency})', invocations=self._invocations_served, current_concurrency=self._current_concurrency, max_concurrency=self._max_concurrency)
        
    @inlineCallbacks
    def onJoin(self, details):
        self._max_concurrency = 10
        self._current_concurrency = 0
        self._invocations_served = 0
        self.user_list = user_list()

        arg_kws = ['color']
        
        reactor.suggestThreadPoolSize(self._max_concurrency)
        verb_translations = {'unhide': 'show',
                             'color': 'setcolor',
                             'keep' : 'remove',
                             'blink' : 'animate',
                             'unblink' : 'unanimate'}
        
        @inlineCallbacks
        def na_query(task,details=None):
            self._invocations_served += 1
            self._current_concurrency += 1

            if not isinstance(task, dict):
                task = json.loads(task)
            task = byteify(task)
            
            user_id = task['user'] if (details.caller_authrole == 'processor' and 'user' in task) \
                      else details.caller
            threshold = None
            if details.progress or 'data_callback_uri' in task: threshold=100 
            if 'verb' in task and task['verb'] not in ['add','show']: threshold=None
            self.log.info("na_query() called with task: {task} ,(current concurrency {current_concurrency} of max {max_concurrency})", current_concurrency=self._current_concurrency, max_concurrency=self._max_concurrency, task=task)

            server = self.user_list.user(user_id)['server']
            (res, succ) = yield threads.deferToThread(server.receive_task, task, threshold)

            print "threads"

            if (not details.caller_authrole == 'processor'):
                try:
                    del task['user_msg']
                except:
                    pass
            if 'user_msg' in task:
                if succ:
                    yield self.call(task['user_msg'], {'info':{'success':
                                                               'Fetching results from NeuroArch'}})
                else:
                    yield self.call(task['user_msg'], {'info':{'error':
                                                               'Error executing query on NeuroArch'}})
            if('data_callback_uri' in task):
                print "callback"
                if('verb' in task and task['verb'] not in ['add','show']):
                    try:
                        task['verb'] = verb_translations[task['verb']]
                    except Exception as e:
                        print e
                        pass
                    
                    try:
                        uri = task['user_msg']
                    except:
                        uri = 'ffbo.ui.receive_msg.%s' % user_id
                        if not(type(uri)==six.text_type): uri = six.u(uri)
                    args = []
                    if 'color' in task: task['color'] = '#' + task['color']
                    for kw in arg_kws:
                        if kw in task: args.append(task[kw])
                    if len(args)==1: args=args[0]
                    yield self.call(uri, {'commands': {task['verb']: [res, args]}})
                    yield self.call(uri, {'info':{'success':'Finished processing command'}})
                else:
                    uri = task['data_callback_uri'] + '.%s' % user_id
                    if not(type(uri)==six.text_type): uri = six.u(uri)
                    for c in res:
                        try:
                            succ = yield self.call(uri, c)
                        except ApplicationError as e:
                            print e
                print "end"
                self.na_query_on_end()
                returnValue(succ)
            else:
                if details.progress:
                    for c in res:
                        details.progress(c)
                    self.na_query_on_end()
                    returnValue({'success': {'info':'Finished fetching all results from database'}})
                else:
                    self.na_query_on_end()
                    returnValue({'success': {'info':'Finished fetching all results from database',
                                                 'data': res}})
        uri = six.u('ffbo.na.query.%s' % str(details.session))
        yield self.register(na_query, uri, RegisterOptions(details_arg='details',concurrency=self._max_concurrency))

        def create_tag(task,details=None):
            if not "tag" in task:
                return {"info":{"error":
                                "Tag must be provided"}}
                
            if not isinstance(task, dict):
                task = json.loads(task)
            task = byteify(task)
            
            user_id = task['user'] if (details.caller_authrole == 'processor' and 'user' in task) \
                      else details.caller
            self.log.info("create_tag() called with task: {task} ",task=task)

            server = self.user_list.user(user_id)['server']
            (output,succ) = server.receive_task({"command":{"retrieve":{"state":0}},"format":"qw"})
            if not succ:
                return {"info":{"error":
                                "There was an error creating the tag"}}
            if isinstance(output, QueryWrapper):
                if 'metadata' in task:
                    succ = output.tag_query_result_node(tag=task['tag'],
                                                        permanent_flag=True,
                                                        **task['metadata'])
                else:
                    succ = output.tag_query_result_node(tag=task['tag'],
                                                        permanent_flag=True)
                if succ==-1:
                    return {"info":{"error":"The tag already exists. Please choose a different one"}}
                else:
                    return {"info":{"success":"tag created successfully"}}
            
            else:
                return {"info":{"error":
                                "No data found in current workspace to create tag"}}
        uri = six.u('ffbo.na.create_tag.%s' % str(details.session))
        yield self.register(create_tag, uri, RegisterOptions(details_arg='details',concurrency=self._max_concurrency))

        def retrieve_tag(task,details=None):
            if not "tag" in task:
                return {"info":{"error":
                                "Tag must be provided"}}
            
            if not isinstance(task, dict):
                task = json.loads(task)
            task = byteify(task)
            
            user_id = task['user'] if (details.caller_authrole == 'processor' and 'user' in task) \
                      else details.caller
            self.log.info("retrieve_tag() called with task: {task} ",task=task)

            server = self.user_list.user(user_id)['server']
            tagged_result = QueryWrapper.from_tag(graph=server.graph, tag=task['tag'])
            if tagged_result and tagged_result['metadata'] and tagged_result['metadata']!='{}':
                server.user.append(tagged_result['qw'])
                return {'data':tagged_result['metadata'],
                        'info':{'success':'Server Retrieved Tag Succesfully'}}
            else:
                return {"info":{"error":
                                "No such tag exists in this database server"}}
            
        uri = six.u('ffbo.na.retrieve_tag.%s' % str(details.session))
        yield self.register(retrieve_tag, uri, RegisterOptions(details_arg='details',concurrency=self._max_concurrency))
        
        # Register a function to retrieve a single neuron information
        def retrieve_neuron(nid):
            self.log.info("retrieve_neuron() called with neuron id: {nid} ", nid = nid)
            res = server.retrieve_neuron(nid)
            print "retrieve neuron result: " + str(res)
            return res

        uri = six.u('ffbo.na.retrieve_neuron.%s' % str(details.session))
        yield self.register(retrieve_neuron, uri,RegisterOptions(concurrency=self._max_concurrency))
        print "registered %s" % uri


#######################
        @inlineCallbacks
        def retrieve_by_id(task,details=None):
            # task is a dictionary of {key_type:string, key:string, user:string}
            self._invocations_served += 1
            self._current_concurrency += 1

            if not isinstance(task, dict):
                task = json.loads(task)
            task = byteify(task)

            user_id = task['user'] if (details.caller_authrole == 'processor' and 'user' in task) \
                      else details.caller
            threshold = None

            if details.progress or 'data_callback_uri' in task: threshold=100 

            server = self.user_list.user(user_id)['server']

            (res, succ) = yield threads.deferToThread(server.retrieve_by_id, task, threshold)
            
            if (not details.caller_authrole == 'processor'):
                try:
                    del task['user_msg']
                except:
                    pass
            if 'user_msg' in task:
                if succ:
                    yield self.call(task['user_msg'], {'info':{'success':
                                                               'Fetching results from NeuroArch'}})
                else:
                    yield self.call(task['user_msg'], {'info':{'error':
                                                               'Error executing query on NeuroArch'}})
            if('data_callback_uri' in task):
                uri = task['data_callback_uri'] + '.%s' % user_id
                for c in res:
                    try:
                        succ = yield self.call(six.u(uri), c)
                    except ApplicationError as e:
                        print e
                self.retrieve_by_id_on_end()
                returnValue(succ)
            else:
                if details.progress:
                    for c in res:
                        details.progress(c)
                    self.retrieve_by_id_on_end()
                    returnValue({'success': {'info':'Finished fetching all results from database'}})
                else:
                    self.retrieve_by_id_on_end()
                    returnValue({'success': {'info':'Finished fetching all results from database',
                                                 'data': res}})
        uri = six.u('ffbo.na.retrieve_by_id.%s' % str(details.session))

        yield self.register(retrieve_by_id, uri, RegisterOptions(details_arg='details',concurrency=self._max_concurrency))

###################


        # Listen for ffbo.processor.connected
        @inlineCallbacks
        def register_component():
            self.log.info( "Registering a component")
            # CALL server registration
            try:
                # registered the procedure we would like to call
                res = yield self.call(six.u('ffbo.server.register'),details.session,'na','na_server_test')
                self.log.info("register new server called with result: {result}",
                                                    result=res)

            except ApplicationError as e:
                if e.error != 'wamp.error.no_such_procedure':
                    raise e

        yield self.subscribe(register_component, six.u('ffbo.processor.connected'))
        self.log.info("subscribed to topic 'ffbo.processor.connected'")

        # Register for memory management pings
        @inlineCallbacks
        def memory_management():
            clensed_users = yield self.user_list.cleanup()
            self.log.info("Memory Manager removed users: {users}", users=clensed_users)
            for user in clensed_users:
                try:
                    yield self.publish(six.u("ffbo.ui.update.%s" % str(user)), "Inactivity Detected, State Memory has been cleared")
                except Exception as e:
                    self.log.warn("Failed to alert user {user} or State Memory removal, with error {e}",user=user,e=e)

        yield self.subscribe(memory_management, six.u('ffbo.processor.memory_manager'))
        self.log.info("subscribed to topic 'ffbo.processor.memory_management'")



        register_component()



if __name__ == '__main__':
    from twisted.internet._sslverify import OpenSSLCertificateAuthorities
    from twisted.internet.ssl import CertificateOptions
    import OpenSSL.crypto
    
                                           
    
    # parse command line parameters
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug output.')
    parser.add_argument('--url', dest='url', type=six.text_type, default=url,
                        help='The router URL (defaults to value from config.py)')
    parser.add_argument('--realm', dest='realm', type=six.text_type, default=realm,
                        help='The realm to join (defaults to value from config.py).')
    parser.add_argument('--ca_cert', dest='ca_cert_file', type=six.text_type,
                        default=ca_cert_file,
                        help='Root CA PEM certificate file (defaults to value from config.py).')
    parser.add_argument('--int_cert', dest='intermediate_cert_file', type=six.text_type,
                        default=intermediate_cert_file,
                        help='Intermediate PEM certificate file (defaults to value from config.py).')
    parser.add_argument('--no-ssl', dest='ssl', action='store_false')
    parser.add_argument('--no-auth', dest='authentication', action='store_false')
    parser.set_defaults(ssl=ssl)
    parser.set_defaults(authentication=authentication)
    parser.set_defaults(debug=debug)
    
    args = parser.parse_args()

    
    # start logging
    if args.debug:
        txaio.start_logging(level='debug')
    else:
        txaio.start_logging(level='info')

   # any extra info we want to forward to our ClientSession (in self.config.extra)
    extra = {'auth': args.authentication}

    if args.ssl:
        st_cert=open(args.ca_cert_file, 'rt').read()
        c=OpenSSL.crypto
        ca_cert=c.load_certificate(c.FILETYPE_PEM, st_cert)
        
        st_cert=open(args.intermediate_cert_file, 'rt').read()
        intermediate_cert=c.load_certificate(c.FILETYPE_PEM, st_cert)
        
        certs = OpenSSLCertificateAuthorities([ca_cert, intermediate_cert])
        ssl_con = CertificateOptions(trustRoot=certs)

        # now actually run a WAMP client using our session class ClientSession
        runner = ApplicationRunner(url=args.url, realm=args.realm, extra=extra, ssl=ssl_con)

    else:
        # now actually run a WAMP client using our session class ClientSession
        runner = ApplicationRunner(url=args.url, realm=args.realm, extra=extra)

    runner.run(AppSession)


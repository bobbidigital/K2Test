''' These objects were written to parse the log files outputed by the K2 Verity
Broker servers. 

K2Configuration -- This parses a configuration document written in XML that
defines how a test should be executed. The XML format is documented in a
separate sample XML configuration file.

K2CommandObject -- This is the base class used to handle test executions. It
wraps the operating system command into a standard object. You can extend the
number of available command types for searching by subclassing this base class
and overriding the execute method. You'll also need to add your class to the
K2CommandObjects dictionary called commands. I know this is disgusting and will
be corrected in the event this test suite is used further.

RCVDKCommand -- This object represents the rcvdk command line tool. These
commands run against the collection via the broker servers.

RCK2Command -- Another object representing the commadn line tool rck2. These
tools run directly against the collections on the file system

K2TestPlan -- This object represents the testplan define in the configuration
file. It is essentially the test runner. It's responsible for thread execution
and monitoring.

K2Report -- A composite object that contains the results of an executed test
K2ReportItem -- Represents a single iteration of a test's result.

'''


from xml.etree import ElementTree
from datetime import datetime
import copy
import threading
import random
import tempfile
import subprocess
import re
import pickle
import Queue
import time


class K2ConfigurationFile(object):

	def __init__(self,file):
		self.config_file = file
		self.config_xml = ElementTree.parse(self.config_file)
		self.global_settings = self.get_global_settings()
		self.test_plans = self.create_test_plans()

	def create_test_plans(self):
		test_threads = self.config_xml.findall('//test')
		test_plans = []
		for test_thread in test_threads:
			K2Test = self.parse_test_element(test_thread)	
			test_plans.append(K2Test)
		return test_plans


	def parse_test_element(self,test_plan):
		try:
			K2Test = K2TestPlan()
			K2Test.threads = int(test_plan.get('threads')) or 1
			K2Test.max_executions = int(test_plan.get('executions')) or None
			K2Test.source_queries = self.parse_text_elements(test_plan,'source_queries/source_query') or self.global_settings['source_queries']
			K2Test.collections = self.parse_text_elements(test_plan,'collections/collection') or self.global_settings['collections']
			K2Test.queries = self.parse_text_elements(test_plan,'queries/query') 
			K2Test.command(self.parse_attributes(test_plan,'command') or self.global_settings['command'])
		except KeyError:
			pass

		return K2Test
	def parse_text_elements(self,etree,xpath):
		elements = etree.findall(xpath)
		return_list = []
		for element in elements:
			return_list.append(element.text.strip())
		return return_list
	def parse_attributes(self,etree,xpath):
		xml = etree.find(xpath)
		if xml is not None:
			attributes = [(key,value) for key,value in xml.attrib.iteritems()]
		else:
			attributes = None
		return attributes

	def get_global_settings(self):
		source_query = self.config_xml.find('source_queries')
		collections = self.config_xml.find('collections')
		parameters = {}
		commands = self.parse_attributes(self.config_xml,'command')
		parameters['command'] = commands 

		for elements in (collections, source_query):
			element_values = []
			for element in elements:
				element_values.append(element.text)
			parameters[elements.tag] = element_values

		return parameters


class K2CommandObject(object):

	@staticmethod
	def get_command_object(parameters):
		commands = { 'rckvdk' : RCVDKCommand,
			     'rck2' : RCK2Command }
		object_type = None
		for parameter in parameters:
			if parameter[0] == 'command':
				object_type = parameter[1]
				break
		if not object_type:
			raise TypeError("command object not specified in tuple %s" % parameters)

		if object_type in commands:
			object = commands[object_type]
			return object(parameters)
		else:
			raise TypeError("%s is not a valid command type" % object_type)

	def __init__(self):
		self.parameters = []
		self.command = ""
		self.source_query = ""
		self.query = ""
		self.collections = []

	def command_line(self):
		command_line = []
		command_line.append(self.command)
		for parameter in self.parameters:
			if not parameter[0] == 'command':
				command_line.append("%s%s" % (self.command_prefix,parameter[0]))
				if parameter[1]: command_line.append(parameter[1])
		return tuple(command_line)

	def execute(self):
		raise NotImplemented()

	def results(self):
		raise NotImplemented()

	def make_tempfile(self):
		f = tempfile.TemporaryFile()
		return f



class RCVDKCommand(K2CommandObject):

	def __init__(self,parameters):
		super(RCVDKCommand,self).__init__()
		self.command = "rcvdk"
		self.command_prefix = "-"
		self.parameters = parameters
		self.time = "-1"
		self.hit_count_regex = 'Retrieved:\s+\d+\((\d+)\)/(\d+)'
		self.elapsed_time_regex = 'Elapsed time is (\d+) milliseconds'
	def execute(self):
		 command = self.command_line()
		 standard_out = self.make_tempfile()
		 standard_in = self.make_tempfile()
		 standard_in.write(self.search_commands())
		 standard_in.seek(0)
		 dt = datetime.now()
		 subprocess.check_call(command,stdout=standard_out, stderr=subprocess.STDOUT, stdin=standard_in)
		 results = self.process_results(standard_out)
		 results['date'] = dt
		 return results

	def process_results(self,std_out):
		std_out.seek(0)
		data = std_out.read()
		searched = 0
		hits = 0
		time = 1
		results = { 'query' : self.query, 'source_query' : '-', 'output' : data }
		matches = re.findall(self.hit_count_regex,data)
		try:
			hits = matches[-1][0]
			searched = matches[-1][1]
		except IndexError:
			searched = matches[-1][0]
			hits = 0
		matches = re.findall(self.elapsed_time_regex,data)
		try:
			time = matches[-1]
		except IndexError:
			pass
		results['time'] = int(time)
		results['searched'] = int(searched)
		results['hits'] = int(hits)
		return results

	def search_commands(self):
		cmds = []
		for collection in self.collections:
			cmds.append("a %s\n" % collection)
		cmds.append('x\ns\nt\ns %s\n' % self.query)
		return ''.join(cmds)



class RCK2Command(RCVDKCommand):
	def __init__(self,parameters):
		super(RCK2Command,self).__init__()
		self.command = "rck2"
		self.command_prefix = "-"
		self.parameters = parameters
		self.hit_count_regex = 'Retrieved:\(\d+\)(\d+)\sof\s(\d+)'
		self.elapsed_time_regex = 'Elapsed time:(\d+)\(ms\)'

	def search_commands(self):
		cmds = []
		cmds.append('c')
		for collection in self.collections:
			cmds.append('\s%s' % collection)
		if self.source_query:
			cmds.append('%s\n' % self.source_query)
		cmds.append('s %s\n' % self.query)
		cmds.append('q\n')
		return ''.join(cmds)


class K2TestPlan(object):
	class K2Thread(threading.Thread):
		def __init__(self,queue,queuelock):
			super(K2TestPlan.K2Thread,self).__init__()
			self.queue = queue
			self.queuelock = queuelock
			self.test_results = []

		def get_command(self):
			self.queuelock.acquire()
			command_object = self.queue.get()
			self.queuelock.release()
			return command_object

		def run(self):
			result = ''
			count = 0
			while not self.queue.empty():
				command_object = self.get_command()
				try:
					result = command_object.execute()
					count += 1
				except subprocess.CalledProcessError as ex:
					print "Called process failed \n %s" % ex
					result = ex
				finally:
					self.test_results.append(result)
			return self.test_results

	def __init__(self):
		self.threads = 1
		self.max_executions = 1
		self.source_queries = []
		self.queries = []
		self.executions = 0
		self.__commandtype__ = None
		self.collections = []
		self.sourcequery_queue = Queue.Queue()
		self.query_queue = Queue.Queue()	
		self.command_queue = Queue.Queue()

	def command(self,command):
		self.__commandtype__ = command

	def fill_query_queues(self):
		sq = self.randomize_list(self.source_queries)
		for q in sq:
			self.sourcequery_queue.put(q)

		while self.query_queue.qsize() < self.max_executions:
			query = self.randomize_list(self.queries)
			for q in query:
				self.query_queue.put(q)

	def get_sourcequery(self):
		return self.__queueitem__(self.sourcequery_queue)

	def get_query(self):
		return self.__queueitem__(self.query_queue)

	def __queueitem__(self,queue):
		if queue.empty():
			return None
		else:
			return queue.get()

	def load_command_queue(self, max_executions):
		for x in range(self.max_executions):
			command_object = K2CommandObject.get_command_object(self.__commandtype__)
			command_object.collections = self.collections
			command_object.query = self.get_query()
			command_object.source_query = self.get_sourcequery()
			self.command_queue.put(command_object)

	def start_threads(self):
		thread_list = []
		queuelock = threading.RLock()
		for x in range(self.threads):
			command_thread = K2TestPlan.K2Thread(self.command_queue,queuelock)
			command_thread.start()
			thread_list.append(command_thread)
		return thread_list

	def run_test(self):
		self.fill_query_queues()
		results = []
		count = 0
		self.load_command_queue(self.max_executions)
		print "Starting %s thread(s)" % self.threads
		thread_list = self.start_threads()
		for thread in thread_list:
			thread.join()
			results += thread.test_results
		print "All threads complete"
		return results

	def randomize_list(self,object_list):
		temp_list = copy.deepcopy(object_list)
		random.shuffle(temp_list)
		return temp_list


class K2Report(object):

	def __init__(self):
		self.__lineitems__ = []
		self.__sortby__ = 'date'
		self.sortable_fields = ('date','time','hits','results','query','source_query')
		self.field_size = {'date': 28,'query' : 50,'source_query': 50,'time' : 10,'searched' : 10,'hits' : 10}	
		header = []
		for field in ('date','query','source_query','time','searched','hits'):
			header.append("%s," % field)

		self.header = ''.join(header)

	def add(self,result):
		self.__lineitems__.append(result)

	def items(self):
		return self.__lineitems__

	def nextitem(self):
		for item in self.__lineitems__:
			yield item

	def display(self,csv=False):
		print self.header
		for item in self.__lineitems__:
			if csv:
				print item.as_csv()
			else:
				print item.as_string()

	@property
	def sort_by(self):
		return self.__sortby__

	@sort_by.setter
	def sort_by(self,value):
		if value not in self.sortable_fields:
			raise SyntaxError("Only allowed fields are %s" % self.sortable_fields)
		self.__sortby__ = value

	def sort(self):
		self.__lineitems__.sort(key=lambda x: getattr(self,self.__sortby__))

	def save(self,dump_file):
		dump_obj = { 'lineitems' : self.__lineitems__, 'sortby' : self.__sortby__, 'sortablefields' : self.sortable_fields }
		pickle.dump(dump_obj,dump_file,-1)
		dump_file.close()

	def load(self,dump_file):
		dump_obj = pickle.load(dump_file)
		self.__lineitems__ = dump_obj['lineitems']
		self.__sortby__ = dump_obj['sortby']
		self.sortable_fields = dump_obj['sortablefields']


class K2ReportItem(object):

	def __init__(self,results):
		self.report_items = results
		self.field_size = {'date': 28,'query' : 50,'source_query': 50,'time' : 10,'searched' : 10,'hits' : 10}	
		self.fields = ('date','query','source_query','time','searched','hits')

	def as_csv(self):
		report_line = []
		for field in self.fields:
			value = str(getattr(self,field))
			report_line.append("%s," % value)
		return ''.join(report_line)

	def as_string(self):
		report_line = []
		width = 0
		for field in self.fields:
			try:
				width = self.field_size[field]
				value = str(getattr(self,field)).ljust(width)
			except KeyError:
				value = "-".ljust(width)
			report_line.append(value)

		return ''.join(report_line)	

	@property
	def fmtdate(self):
		return self.fmdate
	@property
	def date(self):
		return self.report_items['date']

	@property
	def query(self):
		return self.report_items['query']

	@property
	def source_query(self):
		return self.report_items['source_query']

	@property
	def time(self):
		return self.report_items['time']

	@property
	def searched(self):
		return self.report_items['searched']

	@property
	def hits(self):
		return self.report_items['hits']


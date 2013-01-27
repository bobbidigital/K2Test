import unittest
import os
from K2Test import K2ConfigurationFile ,K2Report,K2ReportItem
import time

class ConfigurationFileTest(unittest.TestCase):

	def setUp(self):
		self.file = os.path.abspath("D:/jeff/K2Testing/testconfig.xml")
		self.config = K2ConfigurationFile(self.file)
		self.global_parameters = self.config.get_global_settings()	
		for plan in self.config.test_plans:
			plan.run_test()

	def test_global_parameters(self):
		tp = self.config.test_plans[0].__command__.parameters
		parameters = [('locale','english'),('command', 'rckvdk')]
		self.assertEqual(parameters,tp)

	def test_global_source_queries(self):
		source_queries = self.global_parameters['source_queries']
		test_answer = []
		test_answer.append("'bpvolume' in <DBALIAS>'")
		self.assertEqual(source_queries, test_answer)

	def test_global_collections(self):
		collections = self.global_parameters['collections']
		test_answer = [r'e:\ver\COLL11\coll_search']
		self.assertEqual(collections,test_answer)
			
	def test_test_plan_count(self):
		self.assertEqual(len(self.config.test_plans),2)

	def test_executions(self):
		value = 0
		test_answers = 3
		for plan in self.config.test_plans:
			value += int(plan.max_executions)
		self.assertEqual(value,test_answers)

	def test_default_source_query(self):
		test_plan = self.config.test_plans[1]
		query = test_plan.source_queries[0]
		self.assertEqual(query, "'bpvolume' in <DBALIAS>'")

	#def test_testplan_time(self):
	#	thread_list = []
	#	count = 0

	#	for thread in self.thread_list:
	#		for result in thread.results:
	#			count += result['time']
	#	self.assertEqual(count,402)
		
	def test_testplan_hits(self):
		count = 0
		test_answer = 27971
		for thread in self.thread_list:
			for result in thread.results:
				count += result['hits']
		self.assertEqual(count,test_answer)

	def test_testplan_docssearched(self):
		value = self.thread_list[0].results[0]['results']
		test_answer = 736379
		self.assertEqual(value,test_answer)
			
	def test_pickle(self):
		report = K2Report()
		f = open('D:/jeff/report.bak','wb')
		for test_plan in self.config.test_plans:
			for result in test_plan.results:
				report.add(K2ReportItem(result))
		report.sort_by = 'date'
		report.save(f)
		report.display()


if __name__ == '__main__':
	unittest.main()

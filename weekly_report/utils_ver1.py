import re
import csv
import os
from math import sqrt
from copy import deepcopy
from functools import reduce
'''
	HELPER FUNCTIONS	
'''
def write_to_csv(data, output='/home/vitran/data_test_jan18/missing_sensor_data.csv',mode='w'):
	#mode = 'w' or 'a'
	if mode == 'w':
		# print(data)
		with open(output,'w') as outfile:
			writer = csv.writer(outfile)
			writer.writerows(data)
		outfile.close()
	elif mode == 'a':
		#only applies to daily report csv file
		print(data)
		date_to_add = data[0]
		all_dates_reversed = []
		with open(output,'r') as outfile:
			for line in reversed(list(csv.reader(outfile))):
				all_dates_reversed.append(line)
		outfile.close()

		index = 0
		all_dates_reversed = all_dates_reversed[:-1]
		cur_date_str = all_dates_reversed[index][0].split('/')
		cur_date = Date(cur_date_str[0], cur_date_str[1],cur_date_str[2])

		while date_to_add < cur_date and index<len(all_dates_reversed):
			index += 1
			cur_date_str = all_dates_reversed[index][0].split('/')
			cur_date = Date(cur_date_str[0], cur_date_str[1],cur_date_str[2])
		if date_to_add != cur_date: # if ==: the date is already recorded
			with open(output,'a') as outfile:
				writer = csv.writer(outfile)
				writer.writerows(data)
			outfile.close()
		
	
# def add_to_csv(data, output='/home/vitran/data_test_jan18/missing_sensor_data.csv'):
# 	print(data)
# 	with open(output,'a') as outfile:
# 		writer = csv.writer(outfile)
# 		writer.writerows(data)
# 	outfile.close()

def safe_cast(val, to_type=float, default='None'):
	try:
		num = to_type(val)
		if num < 0: return default
		else: return num
	except (ValueError, TypeError):
		return default

def is_valid_data(val):
	return isinstance(val, float)

def stringify_datetime(num):
	num_str = str(num)
	if len(num_str)<2:
		num_str = '0' + num_str
	return num_str

def sort_extend(stage, key=None):
	def get_seconds(x):
		hms = x[1].split(':')
		# hms = x[2].split(':') ## use when dir is index 0
		return hms[0]*60*60+hms[1]*60+hms[2]
	ans = sorted(stage, key = lambda x: get_seconds(x))
	return ans

def file_name_from_date(date):
	d,m,y = str(date.get_day()),str(date.get_month()),str(date.get_year())
	if len(d) == 1: d = '0'+d
	if len(m) == 1: m = '0'+m
	return str(y)+str(m)+str(d)

def generate_fix_regex(cur_date, station='ingramcreek'):
	prev_date = station+'_'+file_name_from_date(cur_date.prev_date())
	this_date = station+'_'+file_name_from_date(cur_date)
	next_date = station+'_'+file_name_from_date(cur_date.next_date())
	return prev_date, this_date, next_date

def is_correct_date(s, cur_date):
	check = s.split('/')
	test = Date(check[0], check[1], check[2])
	return test == cur_date

def get_file_path(dirname,cur_date,station):
	all_files = os.listdir(dirname)

	prev_datetime, this_datetime, next_datetime = generate_fix_regex(cur_date, station)
	prev_pattern = re.compile(r'%s00[0-9]+\.csv' % re.escape(prev_datetime)) 
	main_pattern = re.compile(r'%s(0[0-9]|1[0-9]|2[0-9])[0-9]+\.csv' % re.escape(this_datetime))
	next_pattern = re.compile (r'%s00[0-9]+\.csv' % re.escape(next_datetime)) 
		
	all_files = [dirname+i for i in all_files if (main_pattern.match(i) or next_pattern.match(i) or prev_pattern.match(i))]
	print('from get_file_path')
	print(prev_datetime)
	print(this_datetime)
	print(next_datetime)
	return all_files

def deep_copy(object):
	return deepcopy(object)


class Station:
	def __init__(self, cur_date, station_info, dirname_in_out):
		self.cur_date = cur_date
		self.dirname_input = dirname_in_out['input']
		self.dirname_raw_output = dirname_in_out['raw_output']
		self.dirname_stats_output = dirname_in_out['stats_output']
		self.station_name = station_info['name']
		self.salt_load_const = station_info['salt_load_const']
		self.flow_const = station_info['flow_const']
		self.weir_width = station_info['weir_width']
		self.offset = station_info['offset']
		self.flow_power = station_info['flow_power']
		self.temp_name, self.ec_name, self.stage_name = self.get_temp_ec_stage_name()
		self.report = self.init_report()

	def init_report(self):
		report = [['date(mm/dd/yyyy)','time(h:m:s)','temp(F)','ec(uS/cm)','stage(ft)']]
		for h in range(0,24,1):
			for m in range(0,60,15):
				h_str = stringify_datetime(h)
				m_str = stringify_datetime(m)
				timestamp = h_str + ':' + m_str + ':00'
				report.append([self.cur_date,timestamp,None, None, None])
		return report

	def get_temp_ec_stage_name(self):
		temp_name = ec_name = stage_name = None
		if self.station_name == 'hospitalcreek':
			ec_name = 'HO_EC_IS'
			stage_name = 'HO_stage-DA'
			temp_name = 'HO_temp_IS' #'HO_PTemp_DA'
		elif self.station_name == 'ingramcreek':
			ec_name = 'IN_EC_IS'
			stage_name = 'IN_stage-DA'
			temp_name = 'IN_temp_IS' #'IN_PTemp_DA'
		return temp_name, ec_name, stage_name

	def report_helper(self, h,m_idx, rows):
		report_index = h*4+m_idx+1
		self.report[report_index][1] = rows[1]
		if self.temp_name in rows:
			if self.report[report_index][2] is not None: #!= 'NOT_RECORDED':
				print('{} {} temp is repeated'.format(rows[0], rows[1]))
				print(self.report[report_index][2])
				print(rows[3])
			self.report[report_index][2] = rows[3] #safe_cast(rows[3], float) #
			# print(rows)
		elif self.ec_name in rows:
			if self.report[report_index][3] is not None: #!= 'NOT_RECORDED':
				print('{} {} ec is repeated'.format(rows[0], rows[1]))
				print(self.report[report_index][3])
				print(rows[3])
			self.report[report_index][3] = rows[3] #safe_cast(rows[3], float) #
			# print(rows)

		elif self.stage_name in rows:
			if self.report[report_index][4] is not None:# != 'NOT_RECORDED':
				print('{} {} stage is repeated'.format(rows[0], rows[1]))
				print(self.report[report_index][4])
				print(rows[3])
			self.report[report_index][4] = rows[3] #safe_cast(rows[3], float) #

	def salt_load_calc(self, x):
		return x*self.salt_load_const

	def flow_calc(self, x):
		return self.flow_const*self.weir_width*((x-self.offset)**self.flow_power)

	def stddev(self, lst):
		mean = float(sum(lst)) / len(lst)
		return sqrt(float(reduce(lambda x, y: x + y, map(lambda x: (x - mean) ** 2, lst))) / len(lst))

	# def get_daily_report(cur_date, salt_load_daily, flow_daily, ec_daily):
	# 	if salt_load_daily and flow_daily:
	# 		report = [deep_copy(cur_date), sum(salt_load_daily)/len(salt_load_daily), min(salt_load_daily), max(salt_load_daily),stddev(salt_load_daily), sum(flow_daily)/len(flow_daily), min(flow_daily), max(flow_daily), stddev(flow_daily), sum(ec_daily)/len(ec_daily), min(ec_daily), max(ec_daily),stddev(ec_daily)]
	# 	elif salt_load_daily:
	# 		report = [deep_copy(cur_date), sum(salt_load_daily)/len(salt_load_daily), min(salt_load_daily), max(salt_load_daily),stddev(salt_load_daily),None, None, None, None, sum(ec_daily)/len(ec_daily), min(ec_daily), max(ec_daily),stddev(ec_daily)]
	# 	elif flow_daily:
	# 		report = [deep_copy(cur_date), None, None, None, None, sum(flow_daily)/len(flow_daily), min(flow_daily), max(flow_daily), stddev(flow_daily), None, None, None, None]
	# 	else:
	# 		report = [deep_copy(cur_date), None, None, None, None, None, None, None, None, None, None, None, None]
	# 	return report

	def check_missing_timestamps(self):
		for timestamp in self.report:
			if timestamp[2] is None:
				timestamp[2] = 'NOT_RECORDED'
			elif timestamp[3] is None:
				timestamp[2] = 'NOT_RECORDED'

	def collect_raw_data_without_QA(self):
		report_raw = []

		with open(file_name, 'r') as infile:
				csvreader = csv.reader(infile)
				rows = next(csvreader)

				for rows in csvreader:
					# 08/14/2020,15:15:00,HO_EC_IS,-99999,,B
					# print(rows[0], self.cur_date)
					if len(rows)>3 and is_correct_date(rows[0], self.cur_date):
						hm = rows[1].split(':')
						h,m = int(hm[0]), int(hm[1])
						

		write_to_csv(self.report,output=self.dirname_raw_output, mode='w')

	def collect_raw_data_with_QA(self):
		all_files = get_file_path(self.dirname_input,self.cur_date,self.station_name)
		min_to_index = [0,15,30,45]
		print(self.cur_date)

		for file_name in all_files:
			print(file_name)
			with open(file_name, 'r') as infile:
				csvreader = csv.reader(infile)
				rows = next(csvreader)

				for rows in csvreader:
					# 08/14/2020,15:15:00,HO_EC_IS,-99999,,B
					# print(rows[0], self.cur_date)
					if len(rows)>3 and is_correct_date(rows[0], self.cur_date):
						hm = rows[1].split(':')
						h,m = int(hm[0]), int(hm[1])
						
						
						if m in min_to_index: 
							self.report_helper(h, min_to_index.index(m), rows)
						# elif m==15: 
						# 	self.report_helper(h, 2, rows)
						# elif m==30: 
						# 	self.report_helper(h, 3, rows)
						# elif m==45: 
						# 	self.report_helper(h, 4, rows)

		write_to_csv(self.report,output=self.dirname_raw_output, mode='w')

	def calc_daily_stats(self):
		ec_daily = []
		stage_daily = [] 
		for timestamp in self.report:
			if is_valid_data(timestamp[2]):
				ec_daily.append(timestamp[2])
			if is_valid_data(timestamp[3]):
				stage_daily.append(timestamp[3])

		# end of a day
		salt_load_daily = []
		flow_daily = []
		daily_stats = [deep_copy(self.cur_date), 
				None, None, None, None, 
				None, None, None, None]
		
		if ec_daily: 
			salt_load_daily = list(map(self.salt_load_calc,ec_daily))
			daily_stats[1:5] = [sum(salt_load_daily)/len(salt_load_daily), min(salt_load_daily), max(salt_load_daily),self.stddev(salt_load_daily)]
		if stage_daily:
			flow_daily = list(map(self.flow_calc, stage_daily))
			daily_stats[5:] = [sum(flow_daily)/len(flow_daily), min(flow_daily), max(flow_daily), self.stddev(flow_daily)]

		write_to_csv(daily_stats,output=self.dirname_stats_output, mode='a')

class Date:
	def __init__(self, m,d,y):
		if isinstance(m,str): m=int(m)
		if isinstance(d,str): d=int(d)
		if isinstance(y,str): y=int(y)
		self.date = d
		self.month = m
		self.year = y
		self.days_per_month = {1:31, 2:28, 3:31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}
	
	def get_day(self):
		return self.date
	
	def get_month(self):
		return self.month
	
	def get_year(self):
		return self.year
	
	def __repr__(self): 
		return "%d/%d/%d" % (self.month, self.date, self.year)

	def __str__(self):
		return "%d/%d/%d" % (self.month, self.date, self.year)

	def check_bound_of_month(self, num_days, mode='end'): #1-indexed/ not check leap year yet
		if mode == 'end':
			return (self.date+num_days) > self.days_per_month[self.month]
		elif mode == 'start':
			return (self.date-num_days) <= 0
	
	def __add__(self, num_days):
		if (not self.check_bound_of_month(num_days, mode='end')):
			self.date += num_days
			return self
		while (self.check_bound_of_month(num_days, mode='end')) or (num_days):
			leftover_days = self.days_per_month[self.month]-self.date
			num_days -= (leftover_days+1)
			self.month += 1
			if self.month == 13:
				self.month = 1
				self.year +=1
			self.date = 1
		return self

	def __sub__(self, num_days):
		if (not self.check_bound_of_month(num_days, mode='start')):
			self.date -= num_days
			return self
		while (self.check_bound_of_month(num_days, mode='start')):
			num_days -= self.date
			self.month -= 1
			if self.month == 0:
				self.month = 12
				self.year -=1
			self.date = self.days_per_month[self.month]
		return self
	
	def __lt__(self, other):
		if self.year > other.get_year():
			return False
		elif self.year == other.get_year():
			if self.month > other.get_month():
				return False
			elif self.month == other.get_month():
				return self.date < other.get_day()
			else:
				return True
		else: 
			return True
	def __le__(self, other):
		if self.year > other.get_year():
			return False
		elif self.year == other.get_year():
			if self.month > other.get_month():
				return False
			elif self.month == other.get_month():
				return self.date <= other.get_day()
			else:
				return True
		else: 
			return True
	
	def __eq__(self, other):
		return self.date==other.get_day() and self.month==other.get_month() and self.year==other.get_year()
	
	def next_date(self):
# 		next = self+1
# 		ans = Date(self.date, self.month, self.year)
# 		self -= 1
		ans = deepcopy(self)
		return ans+1
		#return deepcopy(self+1)#ans
	
	def prev_date(self):
# 		prev = self-1
# 		ans = Date(self.date, self.month, self.year)
# 		self += 1
		ans = deepcopy(self)
		return ans-1
		#return deepcopy(self-1)
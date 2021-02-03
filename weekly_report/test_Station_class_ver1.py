from utils import *

def main():
	STATION_NAME = 'hospitalcreek'
	SALT_LOAD_CONST = 0.64
	FLOW_CONST = 3.33
	WEIR_WIDTH = 4.45 
	OFFSET = -0.025
	FLOW_POWER = 1.5
	station_info = {'name': STATION_NAME,
					'salt_load_const': SALT_LOAD_CONST,
					'flow_const': FLOW_CONST,
					'weir_width': WEIR_WIDTH,
					'offset': OFFSET,
					'flow_power': FLOW_POWER}

	dirname_input = '/home/nwtquinn/public_ftp/incoming/sjvda-realtime.org/SJVDA/'+station_info['name']+'/' #'/home/vitran/hospitalcreek/'
	dirname_stats_output = None #'/home/nwtquinn/public_ftp/daily_report/weekly_report/output/jan25_to_31_hospital/'+cur_date_output_file_name+'.csv'#'/home/vitran/hosgator_feb1/output/daily_stats.csv'

	start_date = Date(1,25,2021)
	end_date = Date(1,31,2021)
	cur_date = start_date
	while cur_date <= end_date:
		cur_date_output_file_name = file_name_from_date(cur_date)
		dirname_raw_output = '/home/nwtquinn/public_ftp/daily_report/code/weekly_report/output/jan25_to_31_hospital/'+cur_date_output_file_name+'.csv' #'/home/vitran/hosgator_feb1/output/'+cur_date_output_file_name+'.csv'
		dirname_in_out = {'input': dirname_input, 
						'raw_output':dirname_raw_output,
						'stats_output': dirname_stats_output}
		my_station = Station(cur_date, station_info, dirname_in_out)
		
		my_station.collect_raw_data_with_QA()
		# my_station.calc_daily_stats()
		# print(cur_date)

		cur_date += 1


if __name__ == "__main__":
	main()
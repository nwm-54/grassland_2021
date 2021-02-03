from utils import *
from datetime import date

def main():
	ingramcreek_info = {'name': 'ingramcreek',
					'salt_load_const': 0.64,
					'flow_const': 3.33,
					'weir_width': 10.0,
					'offset': -0.001,
					'flow_power': 1.5}


	for station_info in [ingramcreek_info]:
		dirname_input = '/home/nwtquinn/public_ftp/incoming/sjvda-realtime.org/SJVDA/'+station_info['name']+'/'
		dirname_stats_output = None #'/home/nwtquinn/public_ftp/daily_report/SJVDA_reports/'+station_info['name']+'/'+station_info['name']+'_stats.csv' 
    
		today = date.today()
# 		start_date = Date(8,14,2020)
# 		end_date = Date(1,31,2021)

 		cur_date = Date(1,31,2021)
#		cur_date = Date(today.day, today.month, today.year)-1
		cur_date_output_file_name = file_name_from_date(cur_date)
		dirname_raw_output =  '/home/nwtquinn/public_ftp/daily_report/code/weekly_report/output/'+station_info['name']+'/daily_raw_data/'+station_info['name']+cur_date_output_file_name+'.csv'
		dirname_in_out = {'input': dirname_input, 
    					'raw_output':dirname_raw_output,
    					'stats_output': dirname_stats_output}
    					
		my_station = Station(cur_date, station_info, dirname_in_out)
		my_station.collect_raw_data()
#		my_station.calc_daily_stats()
# 			cur_date +=1
	
	


if __name__ == "__main__":
	main()

from datetime import date
from utils import *

def main():
	STATION_NAME = 'ingramcreek'
	SALT_LOAD_CONST = 0.64
	FLOW_CONST = 3.33
	WEIR_WIDTH = 10.0
	OFFSET = -0.001
	FLOW_POWER = 1.5

	dirname_input = '/home/nwtquinn/public_ftp/incoming/sjvda-realtime.org/SJVDA/ingramcreek/'
	dirname_output = '/home/nwtquinn/public_ftp/daily_report/SJVDA_reports/ingramcreek_daily_report.csv'
	today = date.today()
	
	cur_date = Date(today.day, today.month, today.year)-1
	report = []
	ec_daily = []
	stage_daily = []
	all_files = get_file_path(dirname_input,cur_date,STATION_NAME)
	
	for file_name in all_files:
		with open(file_name, 'r') as infile:
			csvreader = csv.reader(infile)
			rows = next(csvreader)

			for rows in csvreader:
				# 08/14/2020,15:15:00,HO_EC_IS,-99999,,B

				if len(rows)>3 and is_correct_date(rows[0], cur_date):
					if 'IN_EC_IS' in rows:
						value = safe_cast(rows[3], float)
						if value is not None:
							ec_daily.append(value)
					elif 'IN_stage-DA' in rows:
						value = safe_cast(rows[3], float)
						if value is not None:
							stage_daily.append(value)

	#end of a day
	salt_load_daily = []
	flow_daily = []
	
	if ec_daily: 
		salt_load_daily = [salt_load_calc(x, SALT_LOAD_CONST) for x in ec_daily] 
	if stage_daily:
		flow_daily = [flow_calc(x, flow_const=FLOW_CONST, weir_width=WEIR_WIDTH, offset=OFFSET, flow_power=FLOW_POWER) for x in stage_daily] 
	report_daily = get_daily_report(cur_date, salt_load_daily, flow_daily, ec_daily)
	
	report.append(report_daily)

	add_to_csv(report,output=dirname_output)

if __name__ == "__main__":
	main()

import glob
import xlrd
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from DbContext import DbContext
import datetime

context = DbContext()

def find_last_row(xl_sheet):
	for row in range(0,xl_sheet.nrows):
		# print 'row == ', row
		if 'Data as of' in xl_sheet.cell(row,0).value:
			last_row = row		
	return last_row

def check_if_file_exists_and_process(file):
	print 'check_if_file_exists_and_process'
	qry = """ select id from stocks.stocks_file
			where file_name = '{0}'
		""".format(file)
	
	result = context.execute(qry,[])		
	# If the file exists dont process or else process it
	for each in result:		
		break		
	else:		
		qry = """insert into 
					stocks.stocks_file
						(file_name)
					values ('{0}')
					""".format(file)		
		result = context.execute(qry,[])
		return True
	return False

def main():
	
	stock_count = {}
	stock_date_included = {}
	for file in glob.glob('*.xls'):
		print file
		
		process = check_if_file_exists_and_process(file)
		if process:

			xl_workbook = xlrd.open_workbook(file)

			sheet_names = xl_workbook.sheet_names()
			print('Sheet Names', sheet_names)

			xl_sheet = xl_workbook.sheet_by_name(sheet_names[0])

			# Get the date from the xls sheet
			date = xl_sheet.cell(3,1).value
			print 'date == ', date
					
			date = datetime.datetime.strptime(''.join(date.split(',')[-2:]).strip(), "%B %d %Y").date()
			
			# Determine the last row
			last_row = find_last_row(xl_sheet)

			for row in range(6,last_row - 1):
				# Get the stock symbol			
				values = []
				for each in range(27):
					values.append(0 if xl_sheet.cell(row,each).value == '--' else xl_sheet.cell(row,each).value)
				values.append(date)
				print values
				print len(values)
				

				qry = """ INSERT INTO stocks.stocks_data
					(company_name
					,stock_id
					,rating
					,rs_rating
					,eps_rating
					,annual_eps_pct_change
					,next_qtr_eps_change
					,last_qtr_eps_change
					,last_qtr_sales_change
					,roe
					,pe
					,closing_price
					,price_pct_change
					,vol_pct_change
					,vol_1000
					,52_week_high
					,pct_off_high
					,smr_rating
					,pretax_margin
					,mgm_own_pct
					,qtr_eps_cnt_15
					,group_relative_str
					,acc_dis
					,spon_rating
					,div_yield
					,descrip
					,foot_notes				
					,report_dt
					)
					VALUES (%s,%s,%s,%s,%s,
							%s,%s,%s,%s,%s,
							%s,%s,%s,%s,%s,
							%s,%s,%s,%s,%s,
							%s,%s,%s,%s,%s,
							%s,%s,%s
							)
					"""
				context.executemany(qry,[values])								
	print stock_count 
main()

#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''Extracts and saves pharmacist and pharmacy data from the ACP website

  Last Update: 2016-June-06

  Copyright (c) Notices
	2016	Joshua R. Torrance	<studybuffalo@studybuffalo.com>
	
  This software may be used in any medium or format and adapated to
  any purpose under the following terms:
    - You must give appropriate credit, provide a link to the
      license, and indicate if changes were made. You may do so in
      any reasonable manner, but not in any way that suggests the
      licensor endorses you or your use.
    - You may not use the material for commercial purposes.
    - If you remix, transform, or build upon the material, you must 
      distribute your contributions under the same license as the 
      original.
	
  Alternative uses may be discussed on an individual, case-by-case 
  basis by contacting one of the noted copyright holders.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
  OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
  HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
  WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
  OTHER DEALINGS IN THE SOFTWARE.
'''


import os
import datetime
from reppy.cache import RobotsCache
import codecs
import sys
import ConfigParser
from requests import Session
import json
from bs4 import BeautifulSoup
import time


'''Generates a 50 part progress bar with custom text'''
def progress_bar(title, num, denom, n):
	'''Generates progress bar in console.'''
	percent = 100.00 * num / denom
	
	if percent != 100:
		print("%s [%s%s] %.2f%%  \r" % 
			  (title, (n - 1) * "#", (51 - n) * " ", percent)),
		sys.stdout.flush()
		
		n = n + 1 if percent > n * 2 else n
		
		return n
	else:
		print("%s [%s] Complete!" % (title, "#" * 50))

'''Creates AJAX request with the ACP website and returns the requested data'''
def acp_ajax_request(session, post_data):
	response = session.post(
		url = "https://pharmacists.ab.ca/views/ajax",
		data= post_data,
		headers={
			'Referer': 'https://pharmacists.ab.ca'
		}
	)

	json_response = json.loads(response.text)
	json_response = json_response[1]['data']
	json_response = json_response.encode('utf8')

	
	soup = BeautifulSoup(json_response, 'lxml')
	rows = soup.select("table.table-striped tbody tr")
	
	return rows

'''Extracts pharmacist details from a table row'''
def extract_pharmacist_data(row):
	cells = row.find_all("td")
	pharmacist = cells[0].renderContents().strip()
	location = ""
	location_strings = cells[1].strings
	for string in location_strings:
		location += string.strip() + "\n"
	registration = cells[2].renderContents().strip()
	authorizations = cells[3].renderContents().strip()
	restrictions = cells[4].renderContents().decode('utf8').strip()

	return {"pharmacist": pharmacist,
			"location": location,
			"registration": registration,
			"authorizations": authorizations,
			"restrictions": restrictions}

'''Extracts pharmacy details from a table row'''
def extract_pharmacy_data(row):
	cells = row.find_all("td")
	pharmacy = cells[0].renderContents().strip()
	manager = cells[1].renderContents().strip()
	location_contact = []

	for line in cells[2].strings:
		location_contact.append(line.strip())

	try:
		temp_address = location_contact[0].strip()

		comma_pos = temp_address.rfind(",")
		postal = temp_address[comma_pos + 2:]
		temp_address = temp_address[0:comma_pos - 1]

		comma_pos = temp_address.rfind(",")
		city = temp_address[comma_pos + 2:]
		address = temp_address[0:comma_pos]
	except:
		address = ""
		city = ""
		postal = ""

	try:
		phone = location_contact[5].strip()
	except:
		phone = ""

	try:
		fax = location_contact[8].strip()
	except:
		fax = ""

	return {"pharmacy": pharmacy,
			"manager": manager,
			"address": address,
			"city": city,
			"postal": postal,
			"phone": phone,
			"fax": fax}

print("Alberta Pharmacist and Pharmacy Scraper")
print("---------------------------------------")


'''Checks ACP for permission to crawl web page'''
print("Checking robot.txt for permission to crawl...")

robot = RobotsCache()
can_crawl = robot.allowed("https://pharmacists.ab.ca", "Study Buffalo Data Extraction(http://www.studybuffalo.com/dataextraction)")
crawl_delay = robot.delay("https://pharmacists.ab.ca", "Study Buffalo Data Extraction(http://www.studybuffalo.com/dataextraction)")

# Kills script if permission rejected
if can_crawl == False:
	sys.exit()
else:
	print ("Permission Granted!\n")


'''Generates/Accesses file for saving .csv files'''
# Determine script directory
scriptDir = os.path.dirname(os.path.abspath(__file__))

# Generates today's date for saving and extracting files
today = datetime.date.today()
year = today.year
month = "%02d" % today.month
day = "%02d" % today.day
date = "%s-%s-%s" % (year, month, day)

# Creates folder for extracted data if necessary
save_location = os.path.join(scriptDir, "Extracted_Data", date)
save_location = os.path.normpath(save_location)

if not os.path.exists(save_location):
	os.mkdir(save_location)

# Opens csv files to save extracted data
pharmacist_file_path = os.path.join(save_location, "pharmacists.csv")
pharmacist_file_path = os.path.normpath(pharmacist_file_path)
pharmacist_file = codecs.open(pharmacist_file_path, 'w', encoding='utf-8')

pharmacy_file_path = os.path.join(save_location, "pharmacies.csv")
pharmacy_file_path = os.path.normpath(pharmacy_file_path)
pharmacy_file = codecs.open(pharmacy_file_path, 'w', encoding='utf-8')


'''Connects to config file to grab MySQL credentials'''
config = ConfigParser.ConfigParser()
config.read('../../config/python_config.cfg')


'''Connect to MySQL database'''
mysql_user = config.get('mysql_user_sb_acp_ent', 'user')
mysql_password = config.get('mysql_user_sb_acp_ent', 'password')
mysql_db = config.get('mysql_db_sb_acp', 'db')
mysql_host = config.get('mysql_db_sb_acp', 'host')

'''
conn = MySQLdb.connect(user = mysql_user,
						passwd = mysql_password,
						db = mysql_db, 
						host = mysql_host,
						charset='utf8',
						use_unicode=True)
cursor = conn.cursor()
'''


'''Create session with pharmacists.ab.ca'''
session = Session()
session.head("https://pharmacists.ab.ca")


'''Extract Pharmacist Data'''
print ("Extracting pharmacist data...")

start = 0
end = 1602
length = end - start
step = 1
pharmacist_list = []

for i in range (start, end + 1):
	# Create POST data for retrieving pharmacist information
	page_num = str(i)
	post_data = {
		"view_name": "_acp_advance_filter",
		"view_display_id": "block_3",
		"page": ("0,0,0,0,0,0,%s" % page_num)
	}

	# Processes AJAX response and collects list of pharmacists
	try:
		page_data = acp_ajax_request(session, post_data)

		for row in page_data:
			pharmacist_list.append(extract_pharmacist_data(row))
	except:
		pharmacist_list.append({
			"pharmacist": ("Error with page %s" % str(i +1)),
			"location": "",
			"registration": "",
			"authorizations": "",
			"restrictions": ""})

	# Progress Bar
	step = progress_bar("Screening", i - start, length, step)
	
	# Pause request to comply with robots.txt crawl-delay
	time.sleep(crawl_delay)

print("Extraction complete!\n\n")


'''Saving pharmacist data to MySQL and .csv'''
# Create header row for csv file
pharmacist_file.write(('"%s","%s","%s","%s","%s"\n') % 
						("Pharmacist Name", "Pharmacy of Employment", 
						"Registration Type", "Authorizations", 
						"Practice Restrictions/Conditions"))

for item in pharmacist_list:
	pharmacist_file.write(('"%s","%s","%s","%s","%s"\n') % 
						  (item['pharmacist'], item['location'],
						   item['registration'], item['authorizations'], 
						   item['restrictions']))

# Upload to database here


'''Extracting pharmacy data'''
print ("Extracting pharmacy data...")

start = 0
end = 236
length = end - start
step = 1
pharmacy_list = []

for i in range (start, end + 1):
	# Create POST data for retrieving pharmacy information
	page_num = str(i)
	post_data = {
		"view_name": "_acp_advance_filter",
		"view_display_id": "block",
		"page": ("0,%s" % page_num)
	}

	# Processes AJAX response and collects list of pharmacies
	try:
		page_data = acp_ajax_request(session, post_data)

		for row in page_data:
			pharmacy_list.append(extract_pharmacy_data(row))
	except:
		pharmacy_list.append({
			"pharmacy": ("Error with page %s" % str(i +1)),
			"manager": "",
			"address": "",
			"city": "",
			"postal": "",
			"phone": "",
			"fax": ""})

	# Progress Bar
	step = progress_bar("Screening", i - start, length, step)

	# Pause request to comply with robots.txt crawl-delay
	time.sleep(crawl_delay)

print("Extraction complete!\n\n")


'''Saving pharmacy data to MySQL and .csv'''
# Create header row for csv file
pharmacy_file.write(('"%s","%s","%s","%s","%s","%s","%s"\n') % 
					("Pharmacy Name", "Pharmacy Manager", "Address", 
					 "City", "Postal Code", "Phone Number", "Fax Number"))

for item in pharmacy_list:
	pharmacy_file.write(('"%s","%s","%s","%s","%s","%s","%s"\n') % 
						(item['pharmacy'], item['manager'], item['address'], 
						 item['city'], item['postal'], item['phone'], 
						 item['fax']))

#Upload to database here
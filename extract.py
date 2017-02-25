#!/usr/bin/env python3

"""Extracts and saves pharmacist and pharmacy data from the ACP website
    Last Update: 2017-Feb-25
    Copyright (c) Notices
	    2017	Joshua R. Torrance	<studybuffalo@studybuffalo.com>
	
    This program is free software: you can redistribute it and/or 
    modify it under the terms of the GNU General Public License as 
    published by the Free Software Foundation, either version 3 of the 
    License, or (at your option) any later version.
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
    You should have received a copy of the GNU General Public License
    along with this program.  If not, 
    see <http://www.gnu.org/licenses/>.
    SHOULD YOU REQUIRE ANY EXCEPTIONS TO THIS LICENSE, PLEASE CONTACT 
    THE COPYRIGHT HOLDERS.
"""

"""
    STYLE RULES FOR THIS PROGRAM
    Style follows the Python Style Guide (PEP 8) where possible. The 
    following are common standards for reference
    
    COMMENT LINES to max of 72 characters
    PROGRAM LINES to a max of 79 characters
    
    INDENTATION 4 spaces
    STRINGS use quotation marks
    VARIABLES use camelCase
    GLOBAL VARIABLES use lowercase with underscores
    CLASSES use CapWords
    CONSTANTS use UPPERCASE
    FUNCTIONS use lowercase with underscores
    MODULES use lowercase with underscores
    
    ALIGNMENT
        If possible, align with open delminter
        If not possible, indent
        If one indent would align arguments with code in block, use 
            two indents to provide visual differentiation
        Operators should occur at start of line in broken up lines, 
        not at the end of the preceding line
    OPERATORS & SPACING
    Use spacing in equations
        e.g. 1 + 1 = 2
    Do not use spacing in assigning arguments in functions 
        e.g. def foo(bar=1):
"""


from urllib import robotparser
import ConfigParser
import os
import datetime
import codecs
import sys
from requests import Session
import json
from bs4 import BeautifulSoup
import time


def progress_bar(title, curPos, start, stop):
    """Generates progress bar in console."""
    
    # Normalize start, stop, curPos
    curPos = (curPos - start) + 1
    stop = (stop - start) + 1 
    start = 1

    # Determine current progress
    prog = 100.00 * (curPos / stop)
    
    if prog != 100:
        progComp = "#" * math.floor(prog / 2)
        progRem = " " * (50 - math.floor(prog / 2))
        prog = "%.2f%%" % prog
        print (("%s [%s%s] %s  \r" % (title, progComp, progRem, prog)), end="")
        sys.stdout.flush()
    else:
        progComp = "#" * 50
        print("%s [%s] Complete!" % (title, progComp))


def get_permission(robotTxt, url):
    """Checks the specified robot.txt file for access permission."""
    robot = robotparser.RobotFileParser()
    robot.set_url(robotFile)
    robot.read()
    
    can_crawl = robot.can_fetch(
		"Study Buffalo Data Extraction (http://www.studybuffalo.com/dataextraction/)",
		url)
    
    return can_crawl


def acp_ajax_request(session, post_data):
    """Creates AJAX request with ACP website to return requested data"""
    response = session.post(
        url = "https://pharmacists.ab.ca/views/ajax",
        data = post_data,
        headers = {
            'Referer': 'https://pharmacists.ab.ca'
        }
    )
    
    json_response = json.loads(response.text)
    json_response = json_response[1]['data']
    json_response = json_response.encode('utf8')
    
    soup = BeautifulSoup(json_response, 'lxml')
    rows = soup.select("table.table-striped tbody tr")
    
    return rows


def extract_pharmacist_data(row):
    """Extracts pharmacist details from the table row"""
    cells = row.find_all("td")
    pharmacist = cells[0].renderContents().strip()
    location = ""
    location_strings = cells[1].strings
    
    for string in location_strings:
        location += string.strip() + "\n"

    registration = cells[2].renderContents().strip()
    authorizations = cells[3].renderContents().strip()
    restrictions = cells[4].renderContents().decode('utf8').strip()

    return {
        "pharmacist": pharmacist,
        "location": location,
        "registration": registration,
        "authorizations": authorizations,
        "restrictions": restrictions
    }


def extract_pharmacy_data(row):
    """Extracts pharmacy details from the table row"""
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

    return {
        "pharmacy": pharmacy,
        "manager": manager,
        "address": address,
        "city": city,
        "postal": postal,
        "phone": phone,
        "fax": fax
    }

print ("Alberta Pharmacist and Pharmacy Scraper")
print ("---------------------------------------")


# Checks ACP for permission to crawl web page
print ("Checking robot.txt for permission to crawl...")

can_crawl = get_permission(
    "https://pharmacists.ab.ca/robots.txt", 
    "https://pharmacists.ab.ca/views/"
)

# FIGURE OUT CRAWL DELAY

"""
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
"""
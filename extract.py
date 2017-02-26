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


import logging
from urllib import robotparser
import configparser
import os
import datetime
import codecs
import sys
from requests import Session
import json
from bs4 import BeautifulSoup
import time
from unipath import Path

def get_today():
     # Get the date
    today = datetime.date.today()
    year = today.year
    month = "%02d" % today.month
    day = "%02d" % today.day
    date = "%s-%s-%s" % (year, month, day)

    return date

def set_log_properties(conf):
    logLoc = conf.get("rx_list", "log_loc")
    logDebug = True if conf.get("rx_list", "log_debug") == "True" else False
    
    # File Handler Settings
    date = get_today()
    logName = root.child(logLoc, "%s.log" % date).absolute()
    lhFormat = ""
    
    lh = logging.FileHandler(logName, "a")
    lh.setFormatter(lhFormat)

    # Console Handler Settings
    chFormat = logging.Formatter("%(message)s")
        
    ch = logging.StreamHandler()

    ch.setFormatter(chFormat)
    
    log.addHandler(ch)

    # Set levels to debug if logDebug == True
    if logDebug:
        lh.setLevel(logging.DEBUG)
        ch.setLevel(logging.DEBUG)
    else:
        lh.setLevel(logging.INFO)
        ch.setLevel(logging.CRITICAL)
    
    log.addHandler(lh)
    log.addHandler(ch)

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

def get_permission(robotTxt, url, agent):
    """Checks the specified robot.txt file for access permission."""
    robot = robotparser.RobotFileParser()
    robot.set_url(robotTxt)
    robot.read()

    can_crawl = robot.can_fetch(agent, url)
    
    return can_crawl

def generate_session(url, user):
    """Create session with pharmacists.ab.ca"""
    session = Session()
    session.head(url, headers={"user-agent": user})
    
    return session

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

def request_pharmacist_data(ses, crawlDelay):
    """Requests pharmacist data from the ACP website"""
    data = []

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
            page_data = acp_ajax_request(ses, post_data)

            for row in page_data:
                data.append(extract_pharmacist_data(row))
        except:
            data.append({
			    "pharmacist": ("Error with page %s" % str(i +1)),
			    "location": "",
			    "registration": "",
			    "authorizations": "",
			    "restrictions": ""})

	    # Progress Bar

	    # Pause request to comply with robots.txt crawl-delay
        time.sleep(crawlDelay)

    return data

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

def request_pharmacy_data(ses, crawlDelay):
    data = []
    
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
                data.append(extract_pharmacy_data(row))
        except:
            data.append({
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
        time.sleep(crawlDelay)

    print("Extraction complete!\n\n")
    
    return data

def generate_extract_folder(root):
    """Creates the folders for holding the extracted files"""

    print ("Creating extract folder... ", end="")
    extractLoc = root.child("extracts")

    if not extractLoc.exists():
        os.mkdir(extractLoc.absolute())
    
    print ("Complete!")

    return {"root": root, "eLoc": extractLoc, "pLoc": parseLoc}

def save_data(root, pharmacist, pharmacy):
    # Get the date
    today = datetime.date.today()
    year = today.year
    month = "%02d" % today.month
    day = "%02d" % today.day
    date = "%s-%s-%s" % (year, month, day)

def upload_data(root, pharmacist, pharmacy):
    """Upload data to MySQL Database"""
    # Obtain database credentials
    cLoc = root.parent.child("config", "python_config.cfg").absolute()
    
    config = configparser.ConfigParser()
    config.read(cLoc)

    db = config.get("mysql_db_rx", "db")
    host = config.get("mysql_db_rx", "host")
    user = config.get("mysql_user_rx_ent", "user")
    pw = config.get("mysql_user_rx_ent", "password")

    # Connect to database
    print ("Connecting to database... ", end="")
    
    conn = pymysql.connect(host, user, pw, db)
    cursor = conn.cursor()

    print ("Complete!\n")
    
    print ("Uploading pharmacist data... ", end="")

    print ("Complete!")

    print ("Uploading pharmacy data... ", end="")

    print ("Complete!\n")

    conn.close()


print ("\nALBERTA PHARMACIST AND PHARMACY EXTRACTION TOOL")
print ("-----------------------------------------------")

# SET UP VARIABLES
# Get the public config file and set the root directory
pubConfig = configparser.ConfigParser()
pubConfig.read("config.cfg")
root = Path(pubConfig.get("rx_list", "root"))

# Get the private config file
configLoc = root.parent.child("config", "python_config.cfg").absolute()
privConfig = configparser.ConfigParser().read(configLoc)

log = logging.getLogger(__name__)
set_log_properties(pubConfig)

robotName = pubConfig.get("rx_list", "user_agent")

# Checks ACP for permission to crawl web page
print ("Checking robot.txt for permission... ", end="")

can_crawl = get_permission(
    "https://pharmacists.ab.ca/robots.txt", 
    "https://pharmacists.ab.ca/views/",
    robotName
)

crawlDelay = 10 # as per robots.txt on 2017-02-25

if can_crawl == True:
    print ("Granted!\n")

    # Extract data from website
    print ("EXTRACT DATA")
    print ("------------")

    # Generate session with ACP website
    session = generate_session("https://pharmacists.ab.ca", robotName)
    
    # Extract Pharmacist Data
    pharmacistData = extract_pharmacist_data(session)
    """
    # Extract Pharmacy Data
    pharmacyData = extract_pharmacy_data(session)
    
    # Generate directory to save files to
    print ("SAVE EXTRACTED DATA")
    print ("-------------------")

    save_data(root, pharmacistData, pharmacyData)

    print ("UPLOAD EXTRACTED DATA")
    print ("---------------------")

    upload_data(root, pharmacistData, pharmacyData)
    """
else:
   print ("Rejected.")
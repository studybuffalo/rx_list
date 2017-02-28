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

from unipath import Path
import configparser
import logging.config
from urllib import robotparser
import os
import datetime
from requests import Session
import json
from bs4 import BeautifulSoup
import html
import re
import time
import csv
import pymysql


class NewFileHandler(logging.FileHandler):
    """Update FileHandler to name logs the current date"""
    def __init__(self, path, mode = 'a'):
        # Takes the provided path and appends the date as the log name
        filename = Path(path, "%s.log" % today).absolute()
        super(NewFileHandler,self).__init__(filename, mode)

def get_today():
     # Get the date
    today = datetime.date.today()
    year = today.year
    month = "%02d" % today.month
    day = "%02d" % today.day
    date = "%s-%s-%s" % (year, month, day)

    return date

def set_log_properties(conf):
    """Sets up logging settings and returns logger"""
    logDebug = True if conf.get("rx_list", "log_debug") == "True" else False
    
    if logDebug:
        logging.config.fileConfig("logger.cfg")
    else:
        logging.config.fileConfig("logger_debug.cfg")

    log = logging.getLogger(__name__)

    return log

def get_permission(agent):
    """Checks the specified robot.txt file for access permission."""
    txtUrl = "https://pharmacists.ab.ca/robots.txt"
    reqUrl = "https://pharmacists.ab.ca/views/"

    robot = robotparser.RobotFileParser()
    robot.set_url(txtUrl)
    robot.read()

    can_crawl = robot.can_fetch(agent, reqUrl)
    
    return can_crawl

def generate_session(user):
    """Create session with pharmacists.ab.ca"""
    url = "https://pharmacists.ab.ca"

    try:
        session = Session()
        session.head(url, headers={"user-agent": user})
    except Exception as e:
        log.critical(e)
        session = None
        
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
    
    # Returns the data in JSON format
    json_response = json.loads(response.text)
    json_response = json_response[1]['data']
    #json_response = json_response.encode('utf8')
    
    # Extracts out just the table rows containing data
    soup = BeautifulSoup(json_response, 'lxml')
    rows = soup.select("table.table-striped tbody tr")
    
    return rows

def extract_pharmacist_data(row):
    """Extracts pharmacist details from the table row"""
    # Data is contained within the table cells
    cells = row.find_all("td")

    # Pharmacist Name
    pharmacist = cells[0].renderContents().strip().decode("UTF-8")

    # Convert pharmacy cell into individual lines
    location = []
    pharmacy = ""
    address = ""
    city = ""
    postal = ""
    phone = ""
    fax = ""
    
    for line in cells[1].strings:
        location.append(line.strip())

    try:
        pharmacy = html.unescape(location[0])
    except:
        log.warn("Unable to identify pharmacy for %s" % pharmacist)
    
    
    if pharmacy:
        try:
            tempAddress = html.unescape(location[1].strip())
            
            try:
                # Postal Code is the last content after the final comma
                comma_pos = tempAddress.rfind(",")
                postal = tempAddress[comma_pos + 2:]
                tempAddress = tempAddress[0:comma_pos].strip()

                # City is now the last content after the final comma
                comma_pos = tempAddress.rfind(",")
                city = tempAddress[comma_pos + 2:]

                # Address is the remaining information
                address = tempAddress[0:comma_pos]
            except:
                # Failed to split properly, dump contents into address
                address = tempAddress

                # Log issue
                log.warn("Unable to parse address for %s" % pharmacist)
        except:
            log.warn("Unable to find address for %s" % pharmacist)
        
        try:
            phone = re.sub(r"\D", "", location[3])
        except:
            log.warn("Unable to identify phone for %s" % pharmacist)

        try:
            fax = re.sub(r"\D", "", location[4])
        except:
            log.warn("Unable to identify fax for %s" % pharmacist)

    # Registration Status
    registration = cells[2].renderContents().strip().decode("UTF-8")

    # Authorizations
    authorizations = cells[3].renderContents().strip().decode("UTF-8")
    
    if "Addtl Prescribing Authorization" in authorizations:
        apa = 1
    else:
        apa = 0

    if "Administer Drugs by Injection" in authorizations:
        inject = 1
    else:
        inject = 0

    # Restrictions
    restrictions = cells[4].renderContents().strip().decode("UTF-8")

    return {
        "date": today,
        "pharmacist": pharmacist,
        "pharmacy": pharmacy,
        "address": address,
        "city": city,
        "postal": postal,
        "phone": phone,
        "fax": fax,
        "registration": registration,
        "apa": apa,
        "inject": inject,
        "restrictions": restrictions
    }

def request_pharmacist_data(ses, conf, crawlDelay):
    """Requests pharmacist data from the ACP website"""
    data = []

    log.info("STARTING PHARMACIST DATA EXTRACTION")

    i = int(conf.get("rx_list", "pharmacist_start"))
    stopNum = int(conf.get("rx_list", "request_end"))
    stop = 0

    # Loop until 5 blank requests (signalling data end or repeated errors)
    while stop < 1:
	    # Pause request to comply with robots.txt crawl-delay
        time.sleep(crawlDelay)

        # Create POST data for retrieving pharmacist information
        post_data = {
	        "view_name": "_acp_advance_filter",
	        "view_display_id": "block_3",
	        "page": ("0,0,0,0,0,0,%s" % i)
        }

	    # Processes AJAX response and retrieve response
        try:
            log.debug("Requesting page %s" % i)

            page_data = acp_ajax_request(ses, post_data)
        except Exception as e:
            log.warn("Error with request for page %s: %s" % (i, e))
            page_data = []
        
        # Checks if there is data in request; if not, increment stop counter
        if not page_data:
            stop = stop + 1

        # Process AJAX request into a python list
        for row in page_data:
            try:
                data.append(extract_pharmacist_data(row))
            except Exception as e:
                log.warn("Error processing page %s request: %s" % (i, e))

        i = i + 1
    
    log.info("PHARMACIST DATA EXTRACTION COMPLETE")

    return data

def extract_pharmacy_data(row):
    """Extracts pharmacy details from the table row"""
    # Data is contained within the table cells
    cells = row.find_all("td")

    # Pharmacy Name
    pharmacy = cells[0].renderContents().strip().decode("UTF-8")
    pharmacy = html.unescape(pharmacy)

    # Manager
    manager = cells[1].renderContents().strip().decode("UTF-8")

    # Location, Phone, Fax are all in one cell
    location_contact = []

    # Convert cell into individual lines
    for line in cells[2].strings:
        location_contact.append(line.strip())

    # Attempt to split details out of first line
    try:
        tempAddress = html.unescape(location_contact[0].strip())

        # Postal Code is the last content after the final comma
        comma_pos = tempAddress.rfind(",")
        postal = tempAddress[comma_pos + 2:]
        tempAddress = tempAddress[0:comma_pos].strip()

        # City is now the last content after the final comma
        comma_pos = tempAddress.rfind(",")
        city = tempAddress[comma_pos + 2:]

        # City is the remaining information
        address = tempAddress[0:comma_pos]
    except:
        # Failed to split properly, dump contents into address
        address = location_contact[0].strip()
        city = ""
        postal = ""
        
        # Log issue
        log.warn("Unable to parse address for %s" % pharmacy)

    # Phone is typically the sixth entry
    try:
        phone = location_contact[5].strip()
    except:
        phone = ""

        # Log issue
        log.warn("Unable to parse phone for %s" % pharmacy)

    # Fax is typically ninth entry
    try:
        fax = location_contact[8].strip()
    except:
        fax = ""

        # Log issue
        log.warn("Unable to parse fax for %s" % pharmacy)

    return {
        "date": today,
        "pharmacy": pharmacy,
        "manager": manager,
        "address": address,
        "city": city,
        "postal": postal,
        "phone": phone,
        "fax": fax
    }

def request_pharmacy_data(ses, conf, crawlDelay):
    data = []
    
    log.info("STARTING PHARMACY DATA EXTRACTION")

    i = int(conf.get("rx_list", "pharmacy_start"))
    stopNum = int(conf.get("rx_list", "request_end"))
    stop = 0

    # Loop until 5 blank requests (signalling data end or repeated errors)
    while stop < stopNum:
	    # Pause request to comply with robots.txt crawl-delay
        time.sleep(crawlDelay)

        # Create POST data for retrieving pharmacy information
        post_data = {
	        "view_name": "_acp_advance_filter",
	        "view_display_id": "block",
	        "page": ("0,%s" % i)
        }

	    # Processes AJAX response and retrieve response
        try:
            log.debug("Requesting page %s" % i)

            page_data = acp_ajax_request(ses, post_data)
        except Exception as e:
            log.warn("Error with request for page %s: %s" % (i, e))
            page_data = []
        
        # Checks if there is data in request; if not, increment stop counter
        if not page_data:
            stop = stop + 1

        # Process AJAX request into a python list
        for row in page_data:
            try:
                data.append(extract_pharmacy_data(row))
            except Exception as e:
                log.warn("Error processing page %s request: %s" % (i, e))

        i = i + 1
    
    log.info("PHARMACY DATA EXTRACTION COMPLETE")

    return data

def save_data(config, pharmacist, pharmacy):
    savLoc = root.child("extracts")
    
    # Set File Names
    pharmacistLoc = savLoc.child("%s - Pharmacist.csv" % today)
    pharmacyLoc = savLoc.child("%s - Pharmacy.csv" % today)

    # Write Pharmacist File as CSV
    try:
        with open(pharmacistLoc, "w") as file:
            csvFile = csv.writer(
                file,
                delimiter=",", 
                quotechar='"',
                lineterminator="\n",
                quoting=csv.QUOTE_ALL
            )
            
            for row in pharmacist:
                csvFile.writerow([
                    row["pharmacist"],
                    row["pharmacy"],
                    row["address"],
                    row["city"],
                    row["postal"],
                    row["phone"],
                    row["fax"],
                    row["registration"],
                    row["apa"],
                    row["inject"],
                    row["restrictions"]
                ])

            log.info("Pharmacist data written to %s" % pharmacistLoc)
    except Exception as e:
        log.warn(
            "Error writing pharmacist data to %s: %s" % (pharmacistLoc, e)
        )

    # Write Pharmacy File as CSV
    try:
        with open(pharmacyLoc, "w") as file:
            csvFile = csv.writer(
                file,
                delimiter=",",
                quotechar='"', 
                lineterminator="\n",
                quoting=csv.QUOTE_ALL
            )
            
            for row in pharmacy:
                csvFile.writerow([
                     row["pharmacy"],
                     row["manager"],
                     row["address"],
                     row["city"],
                     row["postal"],
                     row["phone"],
                     row["fax"]
                ])
            
            log.info("Pharmacy data written to %s" % pharmacyLoc)
    except Exception as e:
        log.warn("Error writing pharmacy data to %s: %s" % (pharmacyLoc, e))

def upload_data(root, pharmacist, pharmacy):
    """Upload data to MySQL Database"""
    # Obtain database credentials
    cLoc = root.parent.child("config", "python_config.cfg").absolute()
    
    config = configparser.ConfigParser()
    config.read(cLoc)

    db = config.get("rx_list", "db")
    host = config.get("rx_list", "host")
    user = config.get("rx_list", "user")
    pw = config.get("rx_list", "password")
    tablePharmacist = config.get("rx_list", "table_pharmacist")
    tablePharmacy = config.get("rx_list", "table_pharmacy")

    # Connect to database
    log.info("Connecting to %s" % db)
    
    try:
        conn = pymysql.connect(
            host=host,
            user=user,
            passwd=pw,
            db=db,
            charset="utf8"
        )
        
        log.info("Successfully connected to database")

    except Exception as e:
        log.critical("Unable to connected to database %s: %s" % (db, e))

    cursor = conn.cursor()
    
    # Upload data to pharmacist table
    log.info("Uploading pharmacist data to %s" % tablePharmacist)
    
    # Convert pharmacist data to list of list
    data = []

    for row in pharmacist:
        data.append((
            row["date"],
            row["pharmacist"],
            row["pharmacy"],
            row["address"],
            row["city"],
            row["postal"],
            row["phone"],
            row["fax"],
            row["registration"],
            row["apa"],
            row["inject"],
            row["restrictions"]
        ))

    query1 = "INSERT INTO %s " % tablePharmacist
    query2 = (
        "(date, pharmacist, pharmacy, address, city, postal, phone, "
        "fax, registration, apa, inject, restrictions) VALUES "
        "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )
    query = query1 + query2

    try:
        cursor.executemany(query, data)
        log.info("Pharmacist data upload complete!")
    except Exception as e:
        log.critical(
            "Unable to upload pharmacist data to table %s: %s" % 
            (tablePharmacist, e)
        )

    # Upload data to pharmacy table
    log.info("Uploading pharmacy data to %s" % tablePharmacy)
    
    # Convert pharmacist data to list of list
    data = []

    for row in pharmacy:
        data.append((
            row["date"],
            row["pharmacy"],
            row["manager"],
            row["address"],
            row["city"],
            row["postal"],
            row["phone"],
            row["fax"]
        ))

    query1 = "INSERT INTO %s " % tablePharmacy
    query2 = (
        "(date, pharmacy, manager, address, city, postal, phone, fax) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    )
    query = query1 + query2
    
    try:
        cursor.executemany(query, data)
    
        log.info("Pharmacy data upload complete!")
    except Exception as e:
        log.critical(
            "Unable to upload pharmacy data to table %s: %s" % 
            (tablePharmacy, e)
        )
    conn.close()


# SET UP VARIABLES
# Get the current date
today = get_today()

# Get the public config file and set the root directory
config = configparser.ConfigParser()
config.read("config.cfg")
root = Path(config.get("rx_list", "root"))

# Set up logging functions
log = set_log_properties(config)

# Get the program/robot/crawler name
robotName = config.get("rx_list", "user_agent")

# PROGRAM START
log.info("ALBERTA PHARMACIST AND PHARMACY EXTRACTION TOOL STARTED")

# Checks ACP for permission to crawl web page
log.info("Checking robot.txt for permission to crawl")

canCrawl = get_permission(robotName)

crawlDelay = 10 # as per robots.txt on 2017-02-25

if canCrawl == True:
    log.info("Permission to crawl granted")
    
    # EXTRACT DATA FROM WEBSITE
    # Generate session with ACP website
    log.debug("Generating session with ACP website")

    session = generate_session(robotName)
    
    if session:
        # Extract Pharmacist Data
        pharmacistData = request_pharmacist_data(session, config, crawlDelay)
        
        # Extract Pharmacy Data
        pharmacyData = request_pharmacy_data(session, config, crawlDelay)
    
    # SAVING DATA
    # Save data to file    
    save_data(config, pharmacistData, pharmacyData)

    # Upload data to database
    upload_data(root, pharmacistData, pharmacyData)
else:
   log.info("Rejected.")

log.info("ALBERTA PHARMACIST AND PHARMACY EXTRACTION TOOL COMPLETED")
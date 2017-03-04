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


class PharmacistData(object):
    """Takes a row of pharmacist table data and converts to object"""

    def __init__(self, row):
        # Data is contained within the table cells
        cells = row.find_all("td")

        # Pharmacist Name
        pharmacist = cells[0].renderContents().strip().decode("UTF-8")

        # Convert pharmacy cell into individual lines
        location = []

        for line in cells[1].strings:
            location.append(line.strip())

        # Extract Pharmacy
        pharmacy = ""

        try:
            pharmacy = html.unescape(location[0])
        except Exception:
            msg = "Exception identifying pharmacy for %s" % pharmacist
            log.exception(msg)
    
        # Extract Address, City, Postal Code, Phone and Fax
        address = ""
        city = ""
        postal = ""
        phone = ""
        fax = ""

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
            except Exception:
                log.exception("Unable to find address for %s" % pharmacist)
        
            try:
                phone = re.sub(r"\D", "", location[3])
            except Exception:
                log.exception("Unable to identify phone for %s" % pharmacist)

            try:
                fax = re.sub(r"\D", "", location[4])
            except Exception:
                log.exception("Unable to identify fax for %s" % pharmacist)

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

        self.date = today
        self.pharmacist = pharmacist
        self.pharmacy = pharmacy
        self.address = address
        self.city = city
        self.postal = postal
        self.phone = phone
        self.fax = fax
        self.registration = registration
        self.apa = apa
        self.inject = inject
        self.restrictions = restrictions

class PharmacyData(object):
    pharmacy = ""
    manager = ""
    address = ""
    city = ""
    postal = ""
    phone = ""
    fax = ""

    def __init__(self, row):
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
        except Exception:
            # Failed to split properly, dump contents into address
            address = location_contact[0].strip()
        
            # Log issue
            log.exception("Unable to parse address for %s" % pharmacy)

        # Phone is typically the sixth entry
        try:
            phone = location_contact[5].strip()
        except Exception:
            log.exception("Unable to parse phone for %s" % pharmacy)

        # Fax is typically ninth entry
        try:
            fax = location_contact[8].strip()
        except Exception:
            log.exception("Unable to parse fax for %s" % pharmacy)

        self.date = today
        self.pharmacy = pharmacy
        self.manager = manager
        self.address = address
        self.city = city
        self.postal = postal
        self.phone = phone
        self.fax = fax

def get_today():
    """Returns todays date"""
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
        logging.config.fileConfig("logger_debug.cfg")
    else:
        logging.config.fileConfig("logger.cfg")

    log = logging.getLogger(__name__)
    
    return log

def get_permission(agent):
    """Checks the specified robot.txt file for access permission."""
    class Crawl:
        """Class to contain robot parser output"""
        can = False
        delay = 0

        def __init__(self, can, delay):
            self.can = can
            self.delay = delay
    
    txtUrl = "https://pharmacists.ab.ca/robots.txt"
    reqUrl = "https://pharmacists.ab.ca/views/"

    robot = robotparser.RobotFileParser()
    robot.set_url(txtUrl)
    robot.read()

    can_crawl = robot.can_fetch(agent, reqUrl)
    crawl_delay = 10

    return Crawl(can_crawl, crawl_delay)

def generate_session(user):
    """Create session with pharmacists.ab.ca"""
    url = "https://pharmacists.ab.ca"

    try:
        session = Session()
        session.head(url, headers={"user-agent": user})
    except Exception as e:
        log.exception(e)
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
        except Exception:
            log.exception("Error with request for page %s" % i)
            page_data = []
        
        # Checks if there is data in request; if not, increment stop counter
        if not page_data:
            stop = stop + 1

        # Process AJAX request into a python list
        for row in page_data:
            try:
                data.append(PharmacistData(row))
            except Exception:
                log.exception("Error processing page %s request" % i)

        i = i + 1
    
    log.info("PHARMACIST DATA EXTRACTION COMPLETE")

    return data

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
        except Exception:
            log.exception("Error with request for page %s" % i)
            page_data = []
        
        # Checks if there is data in request; if not, increment stop counter
        if not page_data:
            stop = stop + 1

        # Process AJAX request into a python list
        for row in page_data:
            try:
                data.append(PharmacyData(row))
            except Exception:
                log.exception("Error processing page %s request" % i)

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
            
            for p in pharmacist:
                csvFile.writerow([
                    p.pharmacist,
                    p.pharmacy,
                    p.address,
                    p.city,
                    p.postal,
                    p.phone,
                    p.fax,
                    p.registration,
                    p.apa,
                    p.inject,
                    p.restrictions
                ])

            log.info("Pharmacist data written to %s" % pharmacistLoc)
    except Exception:
        msg = "Error writing pharmacist data to %s" % pharmacistLoc
        log.exception(msg)

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
            
            for p in pharmacy:
                csvFile.writerow([
                     p.pharmacy,
                     p.manager,
                     p.address,
                     p.city,
                     p.postal,
                     p.phone,
                     p.fax
                ])
            
            log.info("Pharmacy data written to %s" % pharmacyLoc)
    except Exception:
        msg = "Error writing pharmacy data to %s" % pharmacyLoc
        log.exception(msg)

def upload_data(root, pharmacist, pharmacy):
    """Upload data to MySQL Database"""
    # Obtain database credentials
    cLoc = root.parent.child("config", "python_config.cfg").absolute()
    
    config = configparser.ConfigParser()
    config.read(cLoc)
    log.debug(cLoc)
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

    except Exception:
        log.exception("Unable to connected to database %s" % db)

    cursor = conn.cursor()
    
    # Upload data to pharmacist table
    log.info("Uploading pharmacist data to %s" % tablePharmacist)
    
    # Convert pharmacist data to list of list
    data = []

    for p in pharmacist:
        data.append((
            p.date,
            p.pharmacist,
            p.pharmacy,
            p.address,
            p.city,
            p.postal,
            p.phone,
            p.fax,
            p.registration,
            p.apa,
            p.inject,
            p.restrictions
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
    except Exception:
        msg = "Unable to upload pharmacist data to table %s" % tablePharmacist
        log.exception(msg)

    # Upload data to pharmacy table
    log.info("Uploading pharmacy data to %s" % tablePharmacy)
    
    # Convert pharmacist data to list of list
    data = []

    for p in pharmacy:
        data.append((
            p.date,
            p.pharmacy,
            p.manager,
            p.address,
            p.city,
            p.postal,
            p.phone,
            p.fax
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
    except Exception:
        msg = "Unable to upload pharmacy data to table %s" % tablePharmacy
        log.critical(msg)

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

crawl = get_permission(robotName)

if crawl.can == True:
    log.info("Permission to crawl granted")
    
    # EXTRACT DATA FROM WEBSITE
    # Generate session with ACP website
    log.debug("Generating session with ACP website")

    session = generate_session(robotName)
    
    if session:
        # Extract Pharmacist Data
        pharmacistData = request_pharmacist_data(session, config, crawl.delay)
        
        # Extract Pharmacy Data
        pharmacyData = request_pharmacy_data(session, config, crawl.delay)
    
    # SAVING DATA
    # Save data to file    
    save_data(config, pharmacistData, pharmacyData)

    # Upload data to database
    upload_data(root, pharmacistData, pharmacyData)
else:
   log.info("Rejected.")

log.info("ALBERTA PHARMACIST AND PHARMACY EXTRACTION TOOL COMPLETED")
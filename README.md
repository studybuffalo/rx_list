# Pharmacist and Pharmacy List
A python program that extracts all pharmacists and pharmacies from the Alberta College of Pharmacists website. The purpose of the program is to generate a more usable list for future projects as well as to track various statistics of pharmacy practice in Alberta.

# Running the Script
- The script needs to be run with the location of the configuration files passed in (makes automation and changes between systems easier).
- Currently the configuration files need to be the root with a logs and extracts folder in the same directory for all files to save properly
- The parent of the root directory needs a folder containing the "config" folder, which contains the "python_config.cfg" file with the required private credentials for the receiving database.

# To Do
- Update configuration files to allow better specification of where items will end up, where to access private configuration files
- Review if logging has been effective in this form or should be reworked

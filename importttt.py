#!/usr/bin/env python
"""
Convert TweakTheTweet spreadsheet into Ushahidi import format and upload it.

Example use:
Download the TweakTheTweet spreadsheet as a csv file and name it: tttin.csv
cd to the directory the csv file is in.
python importttt.py

Sara-Jayne Farmer, Pat Tressel
2012
"""

import requests
#import time
import re
import csv


def upload_report(
        title,
        desc,
        udate,
        uhour,
        umin,
        ampm,
        ucat,
        ulat,
        ulong,
        uloc,
        #first,
        #last,
        #email,
        photo,
        video,
        #news
        ):
    """
    Upload one report.
    """
    
    # Set required parameters for report.
    payload = {'task' : 'report',
               'incident_title'       : title,
               'incident_description' : desc,
               'incident_date'        : udate,
               'incident_hour'        : uhour,
               'incident_minute'      : umin,
               'incident_ampm'        : ampm,
               'incident_category'    : ucat,
               'latitude'             : ulat,
               'longitude'            : ulong,
               'location_name'        : uloc}
    
    # Include optional parameters if provided.
    if photo:
        payload['incident_photo'] = photo
    if video:
        payload['incident_video'] = video

    #Add report to Ushahidi site
    result = requests.post(URL, data=payload)
    
    return(result)


def upload_csv_file():
    """
    Upload TweakTheTweet output csv into Ushahidi via Ushahidi API
    
    The Ushahidi upload function is currently non-functional, so instead,
    each row of the TtT csv file is submitted separately via the Ushahidi API,
    described here:
    https://wiki.ushahidi.com/display/WIKI/Ushahidi+Public+API#UshahidiPublicAPI-SubmittingaReport
    
    @ToDo:
    Eventually, the plan is to have TtT call the Ushahidi API directly rather
    than extracting tweets into a spreadsheet for human entry into Ushahidi.
    Tweets will be inserted with a recognizable category. After tweets are
    submitted, human workers can search for that category and do any cleanup
    needed, including setting the category to whatever is appropriate.
    
    Sara-Jayne Farmer, Pat Tressel
    2012

    """
    
    # The TtT columns are:
    #tttHeadings = ["EVENT", "Report Type", "Report" , "Time - EDT", "Location",
    #               "Text", "Contact", "Details", "Date_Time", "Source",
    #               "COMPLETE", "GPS_Lat", "GPS_Long", "Photo", "Video",
    #               "Author", "ID"]
    
    ERROR_HEADERS = ["Status", "Reason"]
    DEFAULT_LOCATION = "Undefined"

    fin  = open(INFILE, 'rb')
    fout_uploaded = open(OUTFILE_UPLOADED, 'wb')
    fout_rejected = open(OUTFILE_REJECTED, 'wb')
    csvReader = csv.reader(fin)
    csvWriter_uploaded = csv.writer(fout_uploaded)
    csvWriter_rejected = csv.writer(fout_rejected)

    #Pull in TTT file and convert it to Ush format
    headers = csvReader.next()
    csvWriter_uploaded.writerow(headers)
    headers_rejected = headers + ERROR_HEADERS
    csvWriter_rejected.writerow(headers_rejected)
    
    for row in csvReader:
        event_in      = row[0]
        type_in       = row[1]
        report_in     = row[2]
        time_in       = row[3]
        loc_in        = row[4]
        text_in       = row[5]
        #contact_in    = row[6]
        details_in    = row[7]
        #datetime_in   = row[8]
        #source_in     = row[9]
        #complete_in   = row[10]
        lat_in        = row[11]
        long_in       = row[12]
        photo_in      = row[13]
        video_in      = row[14]
        #auth_in       = row[15]
        id_in         = row[16]

        if (EVENTNAME and event_in == EVENTNAME):
            #Use location and title data if it exists
            if loc_in == "NA" or loc_in == "":
                location = DEFAULT_LOCATION
            else:
                location = loc_in
            if report_in == "NA" or report_in == "":
                title = EVENTNAME
            else:
                title = report_in
            if photo_in =="NA":
                photo_in = ""
            if video_in == "NA":
                video_in = ""

            #Create description from text etc
            description = text_in
            #if photo_in != "NA":
            #    description += "Photo: " + photo_in
            #if video_in != "NA":
            #    description += "Video: " + video_in
            if type_in != "NA":
                description += " Type: " + type_in
            if details_in != "NA":
                description += " Details: " + details_in
            description += " TweakTheTweet ID is " + id_in

            # @ToDo: Is it ok to upload Contact and Author, or does that
            # violate privacy? Could these be uploaded but hidden on the
            # map?
            
            # @ToDo: Split datetime_in into udate, uhour, umin, ampm
            #FIXIT: need to deal with dates in formats
            #"a/b/c d:e:f" and "a/b/c d:e"
            #timearr = re.findall("(.+?)/(.+?)/(.+?) (.+?):(.+?)[:(.+?)]?", time_in)
            timearr = re.findall("(.+?)/(.+?)/(.+?) (.+?):(.+?):(.+?)", time_in)
            udate = timearr[0][0] + "/" + timearr[0][1] + "/" + timearr[0][2]
            uhour = timearr[0][3]
            umin = timearr[0][4]
            if int(uhour) > 12:
                ampm = "pm"
                uhour = str(int(uhour) - 12)
            else:
                ampm = "am"
                        
            result = upload_report(
                title,
                description,
                udate,
                uhour,
                umin,
                ampm,
                CATEGORY,
                lat_in,
                long_in,
                location,
                #first,
                #last,
                #email,
                photo_in,
                video_in,
                #news
                )
            
            # Well...did it work?
            if result.status_code == 200:
                # WooHoo!
                csvWriter_uploaded.writerow(row)
            else:
                row.append(result.status_code)
                row.append(result.reason)
                csvWriter_rejected.writerow(row)

    fin.close()
    fout_uploaded.close()
    fout_rejected.close()


if __name__ == '__main__':
    
    # The defaults apply *only* to the Hurricane Isaac map at isaac.fasterthandisaster.org
    import argparse
    parser = argparse.ArgumentParser(
        description = """
        Upload a csv file of Tweak The Tweet records into Ushahidi.
        """,
        usage = """
        python importttt.py --input=[input csv file path]
                            --uploaded=[path for file of successful uploads]
                            --rejected=[path for file of rejected records]
                            --category=[id number of un-verified TtT category]
                            --url=[url of the Ushahidi site]
                            --ttt_event=[name of TtT event; only rows with this name will be used]
                            --start_ttt_id=[TtT id number to begin upload at; only rowis with this TtT id or higher will be used]
        """)
    parser.add_argument(
        "--input", dest="input", default="tttin.csv", help="input csv file path")
    parser.add_argument(
        "--uploaded", dest="uploaded", default="ttt_uploaded.csv", help="path for file of successful uploads")
    parser.add_argument(
        "--rejected", dest="rejected", default="ttt_rejected.csv", help="path for file of rejected uploads")
    parser.add_argument(
        "--category", dest="category", default="67", help="id number of un-verified TtT category")
    parser.add_argument(
        "--url", dest="url", default="https://http://isaac.fasterthandisaster.org/", help="url of the Ushahidi site")
    parser.add_argument(
        "--ttt_event", dest="ttt_event", default="isaac", help="name of TtT event; only rows with this name will be used")
    parser.add_argument(
        "--start_ttt_id", dest="start_ttt_id", default="1", help="TtT id number to begin upload at; only rowis with this TtT id or higher will be used")
    args = vars(parser.parse_args())
    INFILE = args["input"]
    OUTFILE_UPLOADED = args["uploaded"]
    OUTFILE_REJECTED = args["rejected"]
    CATEGORY  = args["category"]
    EVENTNAME = args["ttt_event"]
    BASEURL = args["url"]
    if not BASEURL.endswith("/"):
        BASEURL += "/"
    URL = BASEURL + "api"
    
    # @ToDo: Should we pass in the above parameters to upload_csv_file?
    upload_csv_file()

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
import time
import re
import csv
import sys


# The next TtT id key to accept, and the number of the next sequential file
# to fetch are saved to disk, in case the script exits.  When the script is
# restarted, the greater of the cached values or the command args are used.
NEXT_TTT_ID_KEY = "next_ttt_id"
NEXT_FILE_NUM_KEY = "next_file_num"

# @ToDo: Add constants here for defaults to make them easier to change.
# @ToDo: Add constants for TtT column indices.


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
    result = requests.post(MAP_API_URL, data=payload, **MAP_AUTH)
    
    return(result)


def upload_csv_file(csv_contents, log_uploaded=False, log_rejected=True):
    """ Upload TweakTheTweet output csv into Ushahidi via Ushahidi API

    @param csv_contents: The contents of the csv file, either as an open File
    object or as a list of strings, suitable for processing with csv.reader().
    @param: log_uploaded: If true, lines that were successfully uploaded to the
    Ushahidi server are written to file OUTFILE_UPLOADED.
    @param: log_rejected: If true, lines that were rejected by the Ushahidi
    server are written to file OUTFILE_REJECTED.

    The Ushahidi upload function is currently non-functional, so instead,
    each row of the TtT csv file is submitted separately via the Ushahidi API,
    described here:
    https://wiki.ushahidi.com/display/WIKI/Ushahidi+Public+API#UshahidiPublicAPI-SubmittingaReport

    @ToDo:
    Eventually, the plan is to have TtT call the Ushahidi API directly rather
    than extracting tweets into a spreadsheet for (human) entry into Ushahidi.
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
    
    last_ttt_id = 1  # last TtT id found in this file

    csvReader = csv.reader(csv_contents)
    if log_uploaded:
        fout_uploaded = open(OUTFILE_UPLOADED, 'wb')
        csvWriter_uploaded = csv.writer(fout_uploaded)
    if log_rejected:
        fout_rejected = open(OUTFILE_REJECTED, 'wb')
        csvWriter_rejected = csv.writer(fout_rejected)

    #Pull in TTT file and convert it to Ush format
    headers = csvReader.next()
    if log_uploaded:
        csvWriter_uploaded.writerow(headers)
    if log_rejected:
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
        id_in         = int(row[16])

        if id_in > last_ttt_id:
            last_ttt_id = id_in

        event_in = event_in.lstrip("#").lower()
        if (EVENTNAMES and (event_in in EVENTNAMES) and (id_in >= START_TTT_ID)):
            #Use location and title data if it exists
            if loc_in == "NA" or loc_in == "":
                location = DEFAULT_LOCATION
            else:
                location = loc_in
            if report_in == "NA" or report_in == "":
                title = REPORT_TITLE
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

            category = CATEGORY
            if lat_in == "NA" or lat_in == "":
                lat_in = DEFAULT_LAT
                category = CATEGORY_NO_LAT_LON
            if long_in == "NA" or long_in == "":
                long_in = DEFAULT_LON
                category = CATEGORY_NO_LAT_LON

            result = upload_report(
                title,
                description,
                udate,
                uhour,
                umin,
                ampm,
                category,
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
                if log_uploaded:
                    csvWriter_uploaded.writerow(row)
            else:
                if log_rejected:
                    row.append(result.status_code)
                    row.append(result.reason)
                    csvWriter_rejected.writerow(row)

    if log_uploaded:
        fout_uploaded.close()
    if log_rejected:
        fout_rejected.close()

    return last_ttt_id


def upload_local_csv_file(filepath):
    """ Read one local csv file with TtT data and upload it.
        @param filepath: the path of the csv file.
        @return: last TtT id # in file
    """
    
    try:
        fin = open(filepath, 'rb')
        last_ttt_id = upload_csv_file(fin)
        fin.close()
        return last_ttt_id
    except Exception, e:
        sys.exit("Unable to read csv file %s: %s" % (filepath, e.message))


def upload_remote_csv_file(url):
    """ Read one csv file from the TtT server and upload it.
        @param url: URL with full path of the csv file.
        @return: (status_code, last TtT id # in file)
    """

    last_ttt_id = START_TTT_ID - 1  # In case we can't fetch the file...
    csv_data = requests.get(url)
    # @ToDo: are there other status codes that indicate we got a file?
    if csv_data.status_code == 200:
        csv_lines = csv_data.text.splitlines()
        last_ttt_id = upload_csv_file(csv_lines)
    else:
        # Failure to get a file is normal, as the files are produced at
        # intervals.  Print the status for debugging.
        # @ToDo: Are there any status codes that imply there's no chance we'll
        # get a file later?
        print >> sys.stderr, "Could not fetch %s, got status %d" % (url, csv_data.status_code)
    return (csv_data.status_code, last_ttt_id)


def upload_sequential_remote_csv_files():
    """ Read sequential files from TtT server and upload them. """

    while True:
        # Construct URL for next file.
        ttt_url = "%s%d.csv" % (TTT_URL, START_FILE_NUM)
        # Attempt to upload it.
        (status_code, last_ttt_id) = upload_remote_csv_file(ttt_url)
        # Did we get it?
        if status_code != 200:
            # No, pause.
            time.sleep(FETCH_INTERVAL * 60)
        # Next file.
        START_TTT_ID = last_ttt_id + 1
        START_FILE_NUM += 1
        write_cache(START_TTT_ID, START_FILE_NUM)


def read_cache():
    """ Get saved next TtT id # and next file sequence number, if any
        @return: (nest TtT id #, next file sequence number)
    """

    next_nums = {NEXT_TTT_ID_KEY:1, NEXT_FILE_NUM_KEY:1}
    try:
        cin = open(CACHE_FILE, "r")
        for line in cin:
            key_val = line.split()
            if len(key_val) == 2:
                next_nums[key_val[1]] = int(key_val[2])
        cin.close()
    except:
        pass
    return next_nums


def write_cache(last_ttt_id, last_file_num):
    """ Save next TtT id # and next file sequence number for restart
        @param last_ttt_id: Last TtT id encountered in processed files
        @param last_file_num: Sequence number of last processed file
    """

    next_ttt_id = last_ttt_id + 1 if last_ttt_id >= START_TTT_ID else START_TTT_ID
    next_file_num = last_file_num + 1 if last_file_num >= START_FILE_NUM else START_FILE_NUM
    cout = open(CACHE_FILE, "w")
    cout.write("%s %d\n" % (NEXT_TTT_ID_KEY, next_ttt_id))
    cout.write("%s %d\n" % (NEXT_FILE_NUM_KEY, next_file_num))
    cout.close()


# Sample command line:
#
# python importttt.py \
#   --input_url http://www.cs.colorado.edu/~starbird/TtT_for_Sandy/TtT_records- \
#   --sequential_files \
#   --start_file_number 1 \
#   --fetch_interval 10 \
#   --ttt_events sandy,frankenstorm,njwx,vawx,pawx,ncwx,mdwx,nywx \
#   --map_url https://hurricanesandy.crowdmap.com/ \
#   --map_user joe_mapper \
#   --map_password my!really^secure?password \
#   --category 11 \
#   --category_no_lat_lon 12
#
# @ToDo: Set preepmtive auth flag.
if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser(
        description = """
        Upload csv file(s) of Tweak The Tweet records into Ushahidi.
        """,
        usage = """
        python importttt.py
            --input_file [input csv file path, for a local file]
            --input_url [input csv URL file or prefix, for remote file(s)]
            --sequential_files [whether input_url is a prefix & needs <number>.csv postpended]
            --fetch_interval [minutes to wait between fetch requests]
            --start_file_number [first number to postpend to URL if sequential_files is True]
            --map_url [url of the Ushahidi site]
            --map_user [Ushahidi account to use]
            --map_password [password for Ushahidi account]
            --category [category id number of un-verified TtT records]
            --category_no_lat_lon [category id number of un-verified TtT records without lat lon]
            --default_lat [lat to use for records without lat lon]
            --default_lon [lon to use for records without lat lon]
            --ttt_events [name or list of names of TtT event(s); only rows with these events will be used]
            --report_title [title to use for TtT records that have no report name]
            --start_ttt_id [TtT id number to begin upload at; only rows with this TtT id or higher will be used]
            --uploaded [path for file of successful uploads]
            --rejected [path for file of rejected records]
            --cache_file [path of file script can use to store info for restart]
        Most incident-specific arguments are required and not defaulted,
        with the exception that input_file is mutually exclusive with input_url,
        and postpend_number and fetch_interval are only relevant with input_url.
        """)
    parser.add_argument(
        "--input_file", dest="input_file", help="Input csv file path.")
    parser.add_argument(
        "--input_url", dest="input_url",
        help="URL of remote csv file or URL prefix for sequential files.")
    parser.add_argument(
        "--fetch_interval", dest="fetch_interval", type=int, default=10,
        help="Time in minutes to wait between fetch requests to TtT site. Note if request succeeds and we're reading numbered files, the next file will be tried before waiting, in case this script has fallen behind.")
    parser.add_argument(
        "--sequential_files", dest="sequential_files", action="store_true", default=False,
        help="Include if sequential files should be read from the TtT server. Will postpend a number and .csv to the input_url.")
    parser.add_argument(
        "--start_file_number", dest="start_file_number", type=int, default=1,
        help="First number to postpend to the URL if sequential_files is set. If cache file present, greater of this or value from cache file will be used.")
    parser.add_argument(
        "--uploaded", dest="uploaded", default="ttt_uploaded.csv", help="Path for file of successful uploads")
    parser.add_argument(
        "--rejected", dest="rejected", default="ttt_rejected.csv", help="ath for file of rejected uploads")
    parser.add_argument(
        "--category", dest="category", default="67", help="ategory id number of un-verified TtT records")
    parser.add_argument(
        "--category_no_lat_lon", dest="category_no_lat_lon", default="68", help="ategory id number of un-verified TtT records that lack lat lon")
    parser.add_argument(
        "--default_lat", dest="default_lat", default="0", help="Default lat to use for TtT records without lat lon")
    parser.add_argument(
        "--default_lon", dest="default_lon", default="0", help="Default lon to use for TtT records without lat lon")
    parser.add_argument(
        "--map_url", dest="map_url", help="URL of the Ushahidi site. Use https for security. For Crowdmap or Ushahidi v2.6 or later, https is required.")
    parser.add_argument(
        "--map_user", dest="map_user", help="Account for map site. For Crowdmap, use CrowdmapID account, not an account for the individual map.")
    parser.add_argument(
        "--map_password", dest="map_password", help="Password for map account.")
    parser.add_argument(
        "--ttt_events", dest="ttt_events", default="",
        help="Names of TtT events separated by commas; case and initial # are ignored; if this is specified, only rows with these events will be used")
    parser.add_argument(
        "--report_title", dest="report_title", help="Title to use for TtT records that have no report name")
    parser.add_argument(
        "--start_ttt_id", dest="start_ttt_id", type=int, default=1,
        help="TtT id number to begin upload at; only rows with this TtT id or higher will be used. If cache file present, greater of this or value from cache file will be used.")
    parser.add_argument(
        "--cache_file", dest="cache_file", default="ttt_upload_cache",
        help="Path of a file that the upload script can use to record the file number and ttt id it last processed. Used if the script is halted and restarted.")
    args = vars(parser.parse_args())
    INFILE = args["input_file"]
    TTT_URL = args["input_url"]
    SEQUENTIAL = args["sequential_files"]
    FETCH_INTERVAL = args["fetch_interval"]
    START_FILE_NUM = args["start_file_number"]
    OUTFILE_UPLOADED = args["uploaded"]
    OUTFILE_REJECTED = args["rejected"]
    CATEGORY = args["category"]
    CATEGORY_NO_LAT_LON = args["category_no_lat_lon"]
    DEFAULT_LAT = args["default_lat"]
    DEFAULT_LON = args["default_lon"]
    EVENTNAMES = [event.lstrip("#").lower() for event in args["ttt_events"].split(",")]
    REPORT_TITLE = args["report_title"]
    MAP_BASE_URL = args["map_url"]
    MAP_USER = args["map_user"]
    MAP_PASSWORD = args["map_password"]
    # Support no auth for older maps. Ushahidi 2.6 and later require it.
    if MAP_USER and MAP_PASSWORD:
        MAP_AUTH = {"auth" : "%s@%s" % (MAP_USER, MAP_PASSWORD)}
    else:
        MAP_AUTH = {}
    START_TTT_ID = args["start_ttt_id"]
    CACHE_FILE = args["cache_file"]
    if not MAP_BASE_URL.endswith("/"):
        MAP_BASE_URL += "/"
    MAP_API_URL = MAP_BASE_URL + "api"

    if (INFILE and TTT_URL) or (not INFILE and not TTT_URL):
        print >> sys.stderr, "Specify either input_file or ttt_url."
        sys.exit()

    # Get cached next values if any.
    next_nums = read_cache()
    next_ttt_id = next_nums[NEXT_TTT_ID_KEY]
    next_file_num = next_nums[NEXT_FILE_NUM_KEY]
    if next_ttt_id > START_TTT_ID:
        START_TTT_ID = next_ttt_id
    if next_file_num > START_FILE_NUM:
        START_FILE_NUM = next_file_num

    # @ToDo: Should we pass in the above parameters?
    if INFILE:
        last_ttt_id = upload_local_csv_file(INFILE)
    elif SEQUENTIAL:
        upload_sequential_remote_csv_files()
        # This will not return.
        # @ToDo: Unless we decide there are cases when it should...
        # If so, it should return both the last TtT id and the last file #.
        # Since we'd likely only have it exit on a failure, we don't want to
        # increment START_FILE_NUM as the next file to read.
    else:
        (status_code, last_ttt_id) = upload_remote_csv_file(TTT_URL)

    # The cases that fall out here (single file, or possible failure of the
    # sequential file load, don't need the starting file # incremented.
    write_cache(last_ttt_id + 1, START_FILE_NUM)

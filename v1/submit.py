#!/usr/bin/python

import json
import urllib2
import sys


API_URL = "http://134.74.76.229:8080/post"
RANK_LIMIT = 100

if __name__ == "__main__":
    try:
        lines = open(sys.argv[1]).readlines()
    except:
        print "submit.py filename"
        sys.exit(1)

    influencers = []
    keyword = ""
    for line in lines:
        fields = line.split(":")
        key = fields[0].strip().upper()

        if key == "KEYWORD":
            keyword = fields[1].strip()
            continue

        fields = line.split()
        if len(fields) < 3:
            continue
        rank = int(fields[0])
        if rank <= RANK_LIMIT:
            entity = {"rank": rank,
                      "influencer": fields[1],
                      "connections": fields[2]}
            influencers.append(entity)

    if not keyword:
        print "Keyword not defined in file"
        sys.exit(1)
    if not influencers:
        print "No influencers are defined in file"
        sys.exit(1)

    data = {"influencers": influencers,
            "keyword": keyword}

    data_string = json.dumps(data)

    req = urllib2.Request(API_URL)
    req.add_header('Content-Type', 'application/json')
    response = urllib2.urlopen(req, data_string)
    print "Server code (200 means OK): %d" %response.getcode()
    print "Submitted this json:\n%s" % data_string
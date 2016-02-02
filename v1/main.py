#!/usr/bin/env python

import webapp2
import json
from google.appengine.ext import db
import logging
import time
import operator
from collections import Counter

CUTOFF = 30  # cutoff time for retrieving queued keywords in days
LIMIT = 10  # limit of counted recent searches


def str_clean(str):
    return " ".join(str.split()).lower()


def remove_duplicates(values):
    output = []
    seen = set()
    for value in values:
        # If value has not been encountered yet,
        # ... add it to both list and set.
        if value not in seen:
            output.append(value)
            seen.add(value)
    return output

class Network(db.Model):
    keyword = db.StringProperty(required=True)
    received = db.IntegerProperty(required=True)


class Influencer(db.Model):
    keyword = db.StringProperty(required=True)
    received = db.IntegerProperty(required=True)
    handler = db.StringProperty(required=True)
    rank = db.IntegerProperty(required=True)
    connections = db.IntegerProperty(required=True)
    ci = db.FloatProperty(required=False)


class Keyword(db.Model):
    keyword = db.StringProperty(required=True)
    visited = db.IntegerProperty(required=True)


class Post(webapp2.RequestHandler):
    def get(self):
        self.redirect('/index.html')

    def post(self):
        json_string = self.request.body
        data = json.loads(json_string)

        received = int(time.time())
        keyword = str_clean(data["keyword"])
        influencers = data["influencers"]

        for influencer in influencers:
            handler = str_clean(influencer["influencer"])
            rank = int(influencer["rank"])
            connections = int(influencer["connections"])
            ci = float(influencer["connections"])
            a = Influencer(keyword=keyword,
                           received=received,
                           handler=handler,
                           rank=rank,
                           connections=connections,
                           ci=ci)
            try:
                a.put()
            except:
                self.response.out.write("Error in storing entry %s to database." % handler)
        # save this network to database
        b = Network(keyword=keyword, received=received)
        b.put()


class Get(webapp2.RequestHandler):
    def get(self):
        keyword = str_clean(self.request.get("keyword"))

        if keyword:
            q = Network.all()
            q.filter("keyword =", keyword)
            q.order('-received')
            networks = list(q.run(limit=1))
            if networks:
                network = networks[0]
                output_json = {"keyword": network.keyword,
                               "received": network.received}

                q = Influencer.all()
                q.filter("keyword =", keyword)
                q.filter("received =", networks[0].received)
                q.order("rank")
                # output as json
                influencers = list(q.run())
                influencers_json = list()
                #logging.info(influencers)
                for influencer in influencers:
                    influencer_json = dict()
                    influencer_json["handler"] = influencer.handler
                    influencer_json["rank"] = influencer.rank
                    influencer_json["connections"] = influencer.connections
                    influencers_json.append(influencer_json)

                # record this visit if this network exists
                t = int(time.time())
                a = Keyword(keyword=keyword, visited=t)
                a.put()

                output_json["influencers"] = influencers_json
                output_string = json.dumps(output_json)
                self.response.headers.add_header("Content-Type", "application/json; charset=UTF-8")
                self.response.out.write(output_string)

        else: # no keywork no search
            self.response.out.write("No keyword, no search.")


class GetHistory(webapp2.RequestHandler):
    def get(self):
        keyword = self.request.get("keyword")
        handler = self.request.get("handler")
        if keyword and handler:
            q = Influencer.all()
            q.filter("keyword =", keyword)
            q.filter("handler =", handler)
            q.order("received")

            handler_history = list(q.run())
            history_json = []
            for h in handler_history:
                history = dict()
                history["received"] = h.received
                history["rank"] = h.rank
                history_json.append(history)

            output_json = {"keyword": keyword,
                           "handler": handler,
                           "history": history_json}
            output_string = json.dumps(output_json)
            self.response.headers.add_header("Content-Type", "application/json; charset=UTF-8")
            self.response.out.write(output_string)
        else:
            self.response.out.write("Require keyword and handler to search.")


class GetNetworks(webapp2.RequestHandler):
    def get(self):
        cut = self.request.get("cut")
        if cut:
            cut_seconds = int(86400*float(cut))
        else:
            cut_seconds = int(86400*CUTOFF)

        q = Network.all()
        time_cut = int(time.time()) - cut_seconds
        q.filter("received >", time_cut)
        networks = list(q.run())
        keywords = [x.keyword for x in networks]
        keywords = list(set(keywords))
        #logging.info("keywords)
        output_string = json.dumps(keywords)
        self.response.headers.add_header("Content-Type", "application/json; charset=UTF-8")
        self.response.out.write(output_string)


class Queued(webapp2.RequestHandler):
    def get(self):
        cut = self.request.get("cut")
        if cut:
            cut_seconds = int(86400*float(cut))
        else:
            cut_seconds = int(86400*CUTOFF)

        q = Keyword.all()
        time_cut = int(time.time()) - cut_seconds
        q.filter("visited >", time_cut)
        q.order("-visited")
        networks = list(q.run())
        keywords = [x.keyword for x in networks]
        #logging.warning(repr(keywords))
        keywords = remove_duplicates(keywords)
        #logging.warning(repr(keywords))
        output_string = json.dumps(keywords)
        self.response.headers.add_header("Content-Type", "application/json; charset=UTF-8")
        self.response.out.write(output_string)

    def post(self):
        keyword = self.request.get("keyword")
        t = int(time.time())
        a = Keyword(keyword=keyword, visited=t)
        a.put()


class Hottest(webapp2.RequestHandler):
    def get(self):
        limit = self.request.get("limit")
        if limit:
            limit = int(limit)
        else:
            limit = LIMIT

        cut = self.request.get("cut")
        if cut:
            cut_seconds = int(86400*float(cut))
        else:
            cut_seconds = int(86400*CUTOFF)

        q = Keyword.all()
        time_cut = int(time.time()) - cut_seconds
        q.filter("visited >", time_cut)
        networks = list(q.run())
        keywords = [x.keyword for x in networks]
        #logging.warning(repr(keywords))

        keywords_dict = dict(Counter(keywords))
        sorted_keywords = sorted(keywords_dict.items(), key=operator.itemgetter(1), reverse=True)
        output = [x[0] for x in sorted_keywords]
        output = output[:limit]

        output_string = json.dumps(output)
        self.response.headers.add_header("Content-Type", "application/json; charset=UTF-8")
        self.response.out.write(output_string)


app = webapp2.WSGIApplication([
    ('/post', Post),
    ('/get', Get),
    ('/gethistory', GetHistory),
    ('/getnetworks', GetNetworks),
    ('/queued', Queued),
    ('/hottest', Hottest)
], debug=True)


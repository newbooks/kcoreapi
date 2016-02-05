#!/usr/bin/env python

import webapp2
import json
from google.appengine.ext import db
import time
import operator
from datetime import datetime, timedelta
import logging

CUTOFF = 30  # cutoff time for retrieving queued keywords in days
LIMIT = 10  # limit of counted recent searches


def str_clean(s):
    return " ".join(s.split()).lower()


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
    updated = db.DateTimeProperty(auto_now=True)


class Influencer(db.Model):
    keyword = db.StringProperty(required=True)
    handler = db.StringProperty(required=True)
    rank = db.IntegerProperty(required=True)
    connections = db.IntegerProperty(required=False)
    collective_influence = db.FloatProperty(required=False)
    magnification = db.FloatProperty(required=False)


class Keyword(db.Model):
    keyword = db.StringProperty(required=True)
    last_visited = db.DateTimeProperty(auto_now=True)


class ClickCounters(db.Model):
    keyword = db.StringProperty(required=True)
    day = db.DateProperty(required=True)
    n = db.IntegerProperty(required=True)


class Post(webapp2.RequestHandler):
    def get(self):
        self.redirect('/index.html')

    def post(self):
        json_string = self.request.body
        data = json.loads(json_string)

        received = int(time.time())
        keyword = str_clean(data["keyword"])
        influencers = data["influencers"]

        # Check if this is an update or a new calculation, update network
        q = Network.all()
        q.filter("keyword =", keyword)
        networks = list(q.run(limit=1))  # Only one record for one keyword
        if networks:
            record = networks[0]
        else:
            record = Network(keyword=keyword)
        record.put()

        # delete any existing influencer with this keyword
        q = Influencer.all()
        q.filter("keyword =", keyword)
        records = list(q.run())
        for record in records:
            record.delete()

        # save new influencers
        for influencer in influencers:
            handler = str_clean(influencer["influencer"])
            rank = int(influencer["rank"])
            connections = int(influencer["connections"])
            collective_influence = float(influencer["collective_influence"])
            magnification = float(influencer["magnification"])
            a = Influencer(keyword=keyword,
                           received=received,
                           handler=handler,
                           rank=rank,
                           connections=connections,
                           collective_influence=collective_influence,
                           magnification=magnification)
            a.put()


class Get(webapp2.RequestHandler):
    def get(self):
        keyword = str_clean(self.request.get("keyword"))

        if keyword:
            q = Network.all()
            q.filter("keyword =", keyword)
            networks = list(q.run(limit=1))
            if networks:
                network = networks[0]
                output_json = {"keyword": network.keyword,
                               "received": str(network.updated)}

                q = Influencer.all()
                q.filter("keyword =", keyword)
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
                    influencer_json["collective_influence"] = influencer.collective_influence
                    influencer_json["magnification"] = influencer.magnification

                    influencers_json.append(influencer_json)

                # record this visit in Keywords and ClickCounters if this network exists
                # update keyword visit time (auto add so there is no explicit set)
                q = Keyword.all()
                q.filter("keyword =", keyword)
                keywords = list(q.run(limit=1))
                if keywords:
                    record = keywords[0]
                else:
                    record = Keyword(keyword=keyword)
                record.put()

                # update counters
                q = ClickCounters.all()
                day = datetime.now().date()
                q.filter("day =", day)
                q.filter("keyword =", keyword)
                records = list(q.run(limit=1))
                if records:
                    record = records[0]
                    record.n += 1
                else:
                    record = ClickCounters(keyword=keyword, day=day, n=1)
                record.put()

                output_json["influencers"] = influencers_json
                output_string = json.dumps(output_json)
                self.response.headers.add_header("Content-Type", "application/json; charset=UTF-8")
                self.response.out.write(output_string)

        else:  # no keyword no search
            self.response.out.write("No keyword, no search.")


class GetNetworks(webapp2.RequestHandler):
    def get(self):
        cut = float(self.request.get("cut"))
        if cut:
            d = datetime.now() - timedelta(days=cut)
        else:
            d = datetime.now() - timedelta(days=CUTOFF)

        q = Network.all()
        q.filter("updated >", d)
        networks = list(q.run())
        keywords = [x.keyword for x in networks]
        all_networks = {}

        # load calculated networks
        for network in networks:
            all_networks[network.keyword] = {"keyword": network.keyword,
                 "updated": int((datetime.now() - network.updated).total_seconds()),
                 "clicks": 0,
                 "visited": "Never"}

        # load visited time
        q = Keyword.all()
        q.filter("last_visited >", d)
        networks = list(q.run())
        for network in networks:
            all_networks[network.keyword]["visited"] = int((datetime.now() - network.last_visited).total_seconds())

        # load clicks
        q = ClickCounters.all()
        q.filter("day >=", d)
        networks = list(q.run())
        for network in networks:
            if network.keyword in all_networks:
                all_networks[network.keyword]["clicks"] += network.n

        output_string = json.dumps(all_networks)
        self.response.headers.add_header("Content-Type", "application/json; charset=UTF-8")
        self.response.out.write(output_string)


class Queued(webapp2.RequestHandler):
    def get(self):
        cut = float(self.request.get("cut"))
        if cut:
            d = datetime.now() - timedelta(days=cut)
        else:
            d = datetime.now() - timedelta(days=CUTOFF)

        q = Keyword.all()
        q.filter("last_visited >", d)
        q.order("-last_visited")
        networks = list(q.run())
        keywords = [x.keyword for x in networks]
        #logging.warning(repr(keywords))
        keywords = remove_duplicates(keywords)
        #logging.warning(repr(keywords))
        output_string = json.dumps(keywords)
        self.response.headers.add_header("Content-Type", "application/json; charset=UTF-8")
        self.response.out.write(output_string)

    def post(self):
        keyword = str_clean(self.request.get("keyword"))
        q = Keyword.all()
        q.filter("keyword =", keyword)
        keywords = list(q.run(limit=1))
        if keywords:
            record = keywords[0]
        else:
            record = Keyword(keyword=keyword)
        record.put()


app = webapp2.WSGIApplication([
    ('/post', Post),
    ('/get', Get),
    ('/getnetworks', GetNetworks),
    ('/queued', Queued)
], debug=True)

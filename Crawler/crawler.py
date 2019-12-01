#!/usr/bin/python
#Crawler for Project Applied-Big-Data-Technologies
#with daily Repository Upload at 20:15
#log information Updated +4


import feedparser
import ssl
from html.parser import HTMLParser
from time import gmtime, strftime
import re
import requests
from bs4 import BeautifulSoup
import json
import sqlite3
from datetime import datetime

conn = sqlite3.connect('articles.db')
c = conn.cursor()

lastFazEntryTimestamp = "Tue, 26 Nov 2017 16:28:17 +0100"
lastStuttgarterZeitungEntryTimestamp = "Wed, 27 Nov 2019 07:04:00 GMT"
lastSueddeutscheEntryTimestamp = "Wed, 27 Nov 2019 06:26:26 GMT"

class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return ''.join(self.fed)


def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data().lstrip()

def getGUID(siteName, link):
    if (siteName == "faz"):
        regEx = re.search(r".net\W(.*)", link)
        return regEx[1]
    elif (siteName == "stuttgarter-zeitung"):
        regEx = re.search(r"-.*\.(.*?).html", link)
        return regEx[1]
    elif (siteName == "sueddeutsche"):
        return link

if hasattr(ssl, '_create_unverified_context'):
    ssl._create_default_https_context = ssl._create_unverified_context

urls = ["http://www.faz.net/rss/aktuell/",
        "https://www.stuttgarter-zeitung.de/news.rss.feed",
        "https://rss.sueddeutsche.de/app/service/rss/alles/index.rss?output=rss"
        ]

ID = 0
for url in urls:
    feed = feedparser.parse(url)
    siteHost = url.split("."[0])[1]
    print(len(feed.items()))
    i = 0
    for item in feed:
        print(feed.entries[i])

        dtime = datetime.now()
        d = str(dtime).replace("-", "").replace(" ", "_").replace(":", ".")

        try:
            article_ToP = feed.entries[i].published
        except:
            article_ToP = None

        if (siteHost == "faz"):
            convertedTimestamp = datetime.strptime(lastFazEntryTimestamp, '%a, %d %b %Y %H:%M:%S %z')
            fazTimestamp = datetime.strptime(article_ToP, '%a, %d %b %Y %H:%M:%S %z')
            if (fazTimestamp < convertedTimestamp):
                break
                #print("break")
        elif (siteHost == "stuttgarter-zeitung"):
            convertedTimestamp = datetime.strptime(lastStuttgarterZeitungEntryTimestamp, '%a, %d %b %Y %H:%M:%S %Z')
            stuttgarterZeitungTimestamp = datetime.strptime(article_ToP, '%a, %d %b %Y %H:%M:%S %Z')
            if (stuttgarterZeitungTimestamp < convertedTimestamp):
                break
                #print("break")
        elif (siteHost == "sueddeutsche"):
            convertedTimestamp = datetime.strptime(lastSueddeutscheEntryTimestamp, '%a, %d %b %Y %H:%M:%S %Z')
            sueddeutscheTimestamp = datetime.strptime(article_ToP, '%a, %d %b %Y %H:%M:%S %Z')
            if (sueddeutscheTimestamp < convertedTimestamp):
                break
                #print("break")


        """
            Fetching items from RSS-Feed.
            Declaring variables for each segment.
            Get saved into database.
        """

        article_title = feed.entries[i].title
        article_source = feed.entries[i].title_detail.base
        article_description = strip_tags(feed.entries[i].description)
        article_eLink = feed.entries[i].link
        article_lLink = None
        article_ToC = strftime('%Y-%m-%d %H:%M:%S', gmtime())

        try:
            article_guid = str(getGUID(siteHost, feed.entries[i].guid))
        except:
            article_guid = None





        requestSuccessful = False
        while not requestSuccessful:
            try:
                r = requests.get(article_eLink)
                requestSuccessful = True
            except Exception:
                print("Failed requesting page, Exception: " + str(Exception))

        parsed_html = BeautifulSoup(r.content, "html.parser")

        if (siteHost == "faz"):
            try:
                selectKeywordTag = parsed_html.findAll("meta", {"name": "keywords"})
                keyword = selectKeywordTag[0]['content']
            except:
                keyword = None

            try:
                selectCategoryTag = parsed_html.findAll("div", {"class": "js-adobe-digital-data is-Invisible"})
                result = selectCategoryTag[0]['data-digital-data']
                infoJson = json.loads(result)
                category = infoJson["page"]["ressort"]
                #print(category)
            except:
                category = None

            try:
                results = parsed_html.findAll("div", {"class": "js-adobe-digital-data is-Invisible"})
                result = results[0]['data-digital-data']
                infoJson = json.loads(result)
                author = infoJson["article"]["author"]
            except:
                author = None
        elif (siteHost == "stuttgarter-zeitung"):
            try:
                selectKeywordTag = parsed_html.findAll("meta", {"name": "keywords"})
                keyword = selectKeywordTag[0]['content']
            except:
                keyword = None

            try:
                regExCategory = re.findall(r"'pageRessort':\s'(.*?)'", str(parsed_html))
                category = regExCategory[0]
            except:
                category = None

            try:
                author = feed.entries[i].author
            except:
                author = None

        elif (siteHost == "sueddeutsche"):
            try:
                selectKeywordTag = parsed_html.findAll("meta", {"name": "keywords"})
                keyword = selectKeywordTag[0]['content']
            except:
                keyword = None

            try:
                selectCategoryTag = parsed_html.findAll("script", {"type": "text/javascript"})
                categoryTag = selectCategoryTag[0].decode_contents()
                category = categoryTag.split("[")[1].split("]")[0]
                categoryJson = json.loads(category)
                category = categoryJson["ressort"]
            except:
                category = None

            try:
                author = feed.entries[i].author
            except:
                author = None


        print("______________________________________________")
        print(f"Article uID:\t\t\t\t{str(ID)}")
        ID += 1
        print(f"Site:\t\t\t\t\t{siteHost}")
        print(f"Article title:\t\t\t\t{article_title}")
        print(f"Source:\t\t\t\t\t{article_source}")



        print(f"Article description:\t\t\t{article_description}")
        print(f"Article external link:\t\t\t{article_eLink}")
        print(f"Article keyword:\t\t\t{keyword}")
        print(f"Article category:\t\t\t{category}")

        print(f"Article GuID:\t\t\t\t{article_guid}")

        print(f"Article local link:\t\t\t{article_lLink}")
        print(f"Timestamp of crawl:\t\t\t{article_ToC}")
        print(f"Timestamp of publish:\t\t\t{article_ToP}")

        print(f"Author:\t\t\t\t\t{author}")

        #article text parser
        if (siteHost == "faz"):
            selectedText = parsed_html.findAll('p', {'class': 'atc-TextParagraph'})
            print("------------ SELECTED TEXT -----------")
            extractedText = ""
            for item in selectedText:
                item = item.decode_contents()
                extractedText += re.sub('<[^>]+>', '', item).replace('\n', '')
            print(extractedText)
            print("------------ SELECTED TEXT -----------")
            with open(f"artikel/{d}_{str(siteHost)}_{str(article_guid)}.txt", "w", encoding="utf-8") as f:
                f.write(extractedText)
                f.close()

        elif (siteHost == "stuttgarter-zeitung"):
            # brickgroup mod-article
            dpText = ""
            selectedText = parsed_html.findAll('div', {'class': 'brickgroup mod-article'})
            stringSelectedText = str(selectedText)
            deeperText = re.findall(r'<p>(.*)<\/p>', stringSelectedText)
            for item in deeperText:
                dpText += item
            # print(selectedText)
            print("------------ SELECTED TEXT -----------")
            extractedText = re.sub('<[^>]+>', '', dpText).replace('processBricks();', '')
            print(extractedText)
            print("------------ SELECTED TEXT -----------")
            with open(f"artikel/{d}_{str(siteHost)}_{str(article_guid)}.txt", "w", encoding="utf-8") as f:
                f.write(extractedText)
                f.close()

        elif (siteHost == "sueddeutsche"):
            selectedText = parsed_html.findAll('div', {'class': 'sz-article__body sz-article-body'})
            stringSelectedText = str(selectedText)
            extractedText = re.sub('<[^>]+>', '', stringSelectedText).replace('AdController.render("iqadtile8")',
                                                                              '').replace('\n', '')
            print("------------ SELECTED TEXT -----------")
            print(extractedText)
            print("------------ SELECTED TEXT -----------")
            with open(f"artikel/{d}_{str(siteHost)}_{str(article_guid)}.txt", "w", encoding="utf-8") as f:
                f.write(extractedText)
                f.close()


        # Insert a row of data
        #c.execute(f"INSERT INTO article(authorID, title, description, timestampID, source, external_link, local_link, newspaperID) VALUES (1, '{article_title}', '{article_description}', 1, '{article_source}', '{article_eLink}', '{article_lLink}', 1)")
        #try:
        #    print("Author:                      " + feed.entries[i].author)
        #except:
        #    KeyError
        #    print("Author:                      NA")
        print("______________________________________________")
        i += 1

print(feed.entries)




# Save (commit) the changes +1

#conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.


#conn.close()#!/usr/bin/python3

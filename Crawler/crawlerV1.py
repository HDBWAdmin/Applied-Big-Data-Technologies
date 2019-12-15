#!/usr/bin/python3

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
import mysql.connector
from mysql.connector import Error

#pls dont push these.
connection = mysql.connector.connect(host='*********',
                                     database='*********',
                                     user='*********',
                                     password='*********')


class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.strict = False
        self.convert_charrefs = True
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

cursor = connection.cursor()

# Search for the last added entry to avoid duplicates

sql_select_lastFazEntryTimestamp = "select max(time_of_publication) from article where newspaperID = 51"
cursor.execute(sql_select_lastFazEntryTimestamp)
tListFaz = cursor.fetchall()
lastFazEntryTimestamp = str(tListFaz[0][0])

sql_select_lastStuttgarterZeitungEntryTimestamp = "select max(time_of_publication) from article where newspaperID = 52"
cursor.execute(sql_select_lastStuttgarterZeitungEntryTimestamp)
tListStuttgarterZeitung = cursor.fetchall()
lastStuttgarterZeitungEntryTimestamp = str(tListStuttgarterZeitung[0][0])

sql_select_lastSueddeutscheEntryTimestamp = "select max(time_of_publication) from article where newspaperID = 53"
cursor.execute(sql_select_lastSueddeutscheEntryTimestamp)
tListSueddeutsche = cursor.fetchall()
lastSueddeutscheEntryTimestamp = str(tListSueddeutsche[0][0])


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
        # print(feed.entries[i])

        dtime = datetime.now()
        d = str(dtime).replace("-", "").replace(" ", "_").replace(":", ".")

        try:
            article_ToP = feed.entries[i].published
        except:
            article_ToP = None

        article_eLink = feed.entries[i].link

        host = article_eLink.split("."[0])[1]

        if (host == "faz"):
            convertedTimestamp = datetime.strptime(lastFazEntryTimestamp, '%Y-%m-%d %H:%M:%S')
            fazTimestamp = datetime.strptime(article_ToP, '%a, %d %b %Y %H:%M:%S %z')
            article_ToP = datetime.strptime(article_ToP, '%a, %d %b %Y %H:%M:%S %z').replace(tzinfo=None)
            if (fazTimestamp.replace(tzinfo=None) <= convertedTimestamp):
                break
                # print("break")
        elif (host == "stuttgarter-zeitung"):
            convertedTimestamp = datetime.strptime(lastStuttgarterZeitungEntryTimestamp, '%Y-%m-%d %H:%M:%S')
            stuttgarterZeitungTimestamp = datetime.strptime(article_ToP, '%a, %d %b %Y %H:%M:%S %Z')
            article_ToP = datetime.strptime(article_ToP, '%a, %d %b %Y %H:%M:%S %Z').replace(tzinfo=None)
            if (stuttgarterZeitungTimestamp.replace(tzinfo=None) <= convertedTimestamp):
                break
                # print("break")
        elif (host == "sueddeutsche"):
            convertedTimestamp = datetime.strptime(lastSueddeutscheEntryTimestamp, '%Y-%m-%d %H:%M:%S')
            sueddeutscheTimestamp = datetime.strptime(article_ToP, '%a, %d %b %Y %H:%M:%S %Z')
            article_ToP = datetime.strptime(article_ToP, '%a, %d %b %Y %H:%M:%S %Z').replace(tzinfo=None)
            if (sueddeutscheTimestamp.replace(tzinfo=None) <= convertedTimestamp):
                break
        else:
            break

        article_title = re.sub(r"[^a-zA-Z0-9üäöÜÄÖß-]+", ' ', feed.entries[i].title)
        article_source = feed.entries[i].title_detail.base
        article_description = re.sub(r"[^a-zA-Z0-9üäöÜÄÖß-]+", ' ', strip_tags(feed.entries[i].description))
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
                # print(category)
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

        sql_select_authors = "select author from author"
        cursor.execute(sql_select_authors)
        authorList = cursor.fetchall()
        formattedAuthorList = []
        for auth in authorList:
            formattedAuthorList.append(auth[0])
        if author in formattedAuthorList:
            sql_select_authornumber = "select authorID from author where author='{}'".format(author)
            cursor.execute(sql_select_authornumber)
            authorPosition = cursor.fetchall()
            print("Found Author")
        else:
            sql_insert_author = "insert author (author) values ('{}')".format(author)
            cursor.execute(sql_insert_author)
            connection.commit()
            print(cursor.rowcount, "Record inserted successfully into Author table")
            sql_select_authornumber = "select authorID from author where author='{}'".format(author)
            cursor.execute(sql_select_authornumber)
            authorPosition = cursor.fetchall()
            print("Found no author")

        authorPosition = authorPosition[0][0]
        print("Author Position: " + str(authorPosition))

        sql_select_category = "select category from categories"
        cursor.execute(sql_select_category)
        categoryList = cursor.fetchall()
        formattedCategoryList = []
        for cat in categoryList:
            formattedCategoryList.append(cat[0])
        if category in formattedCategoryList:
            sql_select_categorynumber = "select categoryID from categories where category='{}'".format(category)
            cursor.execute(sql_select_categorynumber)
            categoryPosition = cursor.fetchall()
            print("Found category")
        else:
            sql_insert_category = "insert categories (category) values ('{}')".format(category)
            cursor.execute(sql_insert_category)
            connection.commit()
            print(cursor.rowcount, "Record inserted successfully into Categories table")
            sql_select_categorynumber = "select categoryID from categories where category='{}'".format(category)
            cursor.execute(sql_select_categorynumber)
            categoryPosition = cursor.fetchall()
            print("Found no category")

        categoryPosition = categoryPosition[0][0]
        print("Category Position: " + str(categoryPosition))

        sql_select_newspaper = "select newspaper from newspaper"
        cursor.execute(sql_select_newspaper)
        newspaperList = cursor.fetchall()
        formattedNewspaperList = []
        for newsp in newspaperList:
            formattedNewspaperList.append(newsp[0])
        if siteHost in formattedNewspaperList:
            sql_select_newspapernumber = "select newspaperID from newspaper where newspaper='{}'".format(siteHost)
            cursor.execute(sql_select_newspapernumber)
            newspaperPosition = cursor.fetchall()
            print("Found newspaper")
        else:
            sql_insert_newspaper = "insert newspaper (newspaper) values ('{}')".format(siteHost)
            cursor.execute(sql_insert_newspaper)
            connection.commit()
            print(cursor.rowcount, "Record inserted successfully into Newspaper table")
            sql_select_newspaperynumber = "select newspaperID from newspaper where newspaper='{}'".format(siteHost)
            cursor.execute(sql_select_newspaperynumber)
            newspaperPosition = cursor.fetchall()
            print("Found no newspaper")

        newspaperPosition = newspaperPosition[0][0]
        print("Newspaper Position: " + str(newspaperPosition))

        sql_select_keyword = "select keyword from keywords"
        cursor.execute(sql_select_keyword)
        keywordList = cursor.fetchall()
        formattedKeywordList = []
        for keyw in keywordList:
            formattedKeywordList.append(keyw[0])
        if keyword in formattedKeywordList:
            sql_select_keywordnumber = "select keywordID from keywords where keyword='{}'".format(keyword)
            cursor.execute(sql_select_keywordnumber)
            keywordPosition = cursor.fetchall()
            print("Found keyword")
        else:
            sql_insert_keyword = "insert keywords (keyword) values ('{}')".format(keyword)
            cursor.execute(sql_insert_keyword)
            connection.commit()
            print(cursor.rowcount, "Record inserted successfully into Keywords table")
            sql_select_keywordnumber = "select keywordID from keywords where keyword='{}'".format(keyword)
            cursor.execute(sql_select_keywordnumber)
            keywordPosition = cursor.fetchall()
            print("Found no keyword")

        keywordPosition = keywordPosition[0][0]
        print("Keyword Position: " + str(keywordPosition))

        # article_txt_parser
        if (siteHost == "faz"):
            selectedText = parsed_html.findAll('p', {'class': 'atc-TextParagraph'})
            extractedText = ""
            for item in selectedText:
                item = item.decode_contents()
                extractedText += re.sub('<[^>]+>', '', item).replace('\n', '')
            with open(f"/home/adminhdbw/Desinformation/artikel/{d}_{str(siteHost)}_{str(article_guid)}.txt", "w", encoding="utf-8") as f:
                f.write(extractedText)
                f.close()

        elif (siteHost == "stuttgarter-zeitung"):
            dpText = ""
            selectedText = parsed_html.findAll('div', {'class': 'brickgroup mod-article'})
            stringSelectedText = str(selectedText)
            deeperText = re.findall(r'<p>(.*)<\/p>', stringSelectedText)
            for item in deeperText:
                dpText += item

            extractedText = re.sub('<[^>]+>', '', dpText).replace('processBricks();', '')

            with open(f"/home/adminhdbw/Desinformation/artikel/{d}_{str(siteHost)}_{str(article_guid)}.txt", "w", encoding="utf-8") as f:
                f.write(extractedText)
                f.close()

        elif (siteHost == "sueddeutsche"):
            selectedText = parsed_html.findAll('div', {'class': 'sz-article__body sz-article-body'})
            stringSelectedText = str(selectedText)
            extractedText = re.sub('<[^>]+>', '', stringSelectedText).replace('AdController.render("iqadtile8")',
                                                                              '').replace('\n', '')

            with open(f"/home/adminhdbw/Desinformation/artikel/{d}_{str(siteHost)}_{str(article_guid)}.txt", "w", encoding="utf-8") as f:
                f.write(extractedText)
                f.close()

        article_lLink = ("artikel/" + d + str(siteHost) + "_" + str(article_guid) + ".txt")

        # article insert
        sql_insert_article = "insert article (authorID, newspaperID, title, description, source, external_link, local_link, time_of_publication, time_of_crawl) values ({}, {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(authorPosition, newspaperPosition, article_title, article_description, article_source, article_eLink, article_lLink, article_ToP, article_ToC)
        cursor.execute(sql_insert_article)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into Article table")
        articleID = cursor.lastrowid

        # article_categories insert
        sql_insert_article_categories = "insert article_categories (categoryID, articleID) VALUES ('{}', '{}')".format(categoryPosition, articleID)
        cursor.execute(sql_insert_article_categories)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into Article_Category table")

        # article_keywords insert
        sql_insert_article_keywords = "insert article_keywords (keywordID, articleID) VALUES ('{}', '{}')".format(keywordPosition, articleID)
        cursor.execute(sql_insert_article_keywords)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into Article_Keyword table")

        # Output_CLI
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
        print("______________________________________________")
        i += 1

if (connection.is_connected()):
    connection.close()
    cursor.close()
    print("MySQL connection is closed")


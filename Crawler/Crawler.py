import MLStripper
import DBCredential
import Article
import mysql.connector
import feedparser
from datetime import datetime
from time import gmtime, strftime
import re
import requests
from bs4 import BeautifulSoup
import json
import time
import os

#The three newspaper we want to crawl
urls = ["http://www.faz.net/rss/aktuell/",
        "https://www.stuttgarter-zeitung.de/news.rss.feed",
        "https://rss.sueddeutsche.de/app/service/rss/alles/index.rss?output=rss"
        ]

def strip_tags(html):
    s = MLStripper.MLStripper()
    s.feed(html)
    return s.get_data().lstrip()

#Method to request the article link and return the HTML
def requestPage(articleLink):
    requestSuccessful = False
    while not requestSuccessful:
        try:
            r = requests.get(articleLink)
            requestSuccessful = True
        except Exception:
            print("Failed requesting page, Exception: " + str(Exception))
            time.sleep(5)

    return BeautifulSoup(r.content, "html.parser")

#Local Path where the article text should be saved
localPath = "/home/adminhdbw/Desinformation/artikel"

#Check if the stated path is correct and exists
if (os.path.isdir(localPath)):
    print("The choosen path exists")
else:
    raise Exception('The given path is not exisiting')


credentials = DBCredential.DBCredential()

#Establishing connection to database
connection = mysql.connector.connect(host=credentials.host,
                                     database=credentials.database,
                                     user=credentials.user,
                                     password=credentials.password)

#Initialzing cursor
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


def crawl():
    try:
        for url in urls:
            feed = feedparser.parse(url)
            #siteHost = url.split("."[0])[1]
            print("The current feed length is " + str(len(feed.items())))
            i = 0
            for item in feed:

                #Datetime for file saving
                dtime = datetime.now()
                d = str(dtime).replace("-", "").replace(" ", "_").replace(":", ".")

                #Creating new article object for every article that gets crawled
                article = Article.Article()

                #Catching service current time
                #dtime = datetime.now()
                #Formatting datetime to right timeformat
                #d = str(dtime).replace("-", "").replace(" ", "_").replace(":", ".")


                article.timeOfPublish = feed.entries[i].published
                article.link = feed.entries[i].link
                article.host = article.link.split("."[0])[1]

                #Checking if the timestamp of the crawled article is newer compared to the newest in the database of that newspaper
                if (article.host == "faz"):
                    #Convert last Timestamp to the right format to compare it to the Article Time Of Publish
                    convertedTimestamp = datetime.strptime(lastFazEntryTimestamp, '%Y-%m-%d %H:%M:%S')
                    fazTimestamp = datetime.strptime(article.timeOfPublish, '%a, %d %b %Y %H:%M:%S %z')
                    article.timeOfPublish = datetime.strptime(article.timeOfPublish, '%a, %d %b %Y %H:%M:%S %z').replace(tzinfo=None)
                    #If the timestamp is older than our newest in the database it skips the article
                    if (fazTimestamp.replace(tzinfo=None) <= convertedTimestamp):
                        i += 1
                        continue
                elif (article.host == "stuttgarter-zeitung"):
                    # Convert last Timestamp to the right format to compare it to the Article Time Of Publish
                    convertedTimestamp = datetime.strptime(lastStuttgarterZeitungEntryTimestamp, '%Y-%m-%d %H:%M:%S')
                    stuttgarterZeitungTimestamp = datetime.strptime(article.timeOfPublish, '%a, %d %b %Y %H:%M:%S %Z')
                    article.timeOfPublish = datetime.strptime(article.timeOfPublish, '%a, %d %b %Y %H:%M:%S %Z').replace(tzinfo=None)
                    # If the timestamp is older than our newest in the database it skips the article
                    if (stuttgarterZeitungTimestamp.replace(tzinfo=None) <= convertedTimestamp):
                        i += 1
                        continue
                elif (article.host == "sueddeutsche"):
                    # Convert last Timestamp to the right format to compare it to the Article Time Of Publish
                    convertedTimestamp = datetime.strptime(lastSueddeutscheEntryTimestamp, '%Y-%m-%d %H:%M:%S')
                    sueddeutscheTimestamp = datetime.strptime(article.timeOfPublish, '%a, %d %b %Y %H:%M:%S %Z')
                    article.timeOfPublish = datetime.strptime(article.timeOfPublish, '%a, %d %b %Y %H:%M:%S %Z').replace(tzinfo=None)
                    #If the timestamp is older than our newest in the database it skips the article
                    if (sueddeutscheTimestamp.replace(tzinfo=None) <= convertedTimestamp):
                        i += 1
                        continue
                else:
                    i += 1
                    continue

                article.title = re.sub(r"[^a-zA-Z0-9üäöÜÄÖß-]+", ' ', feed.entries[i].title)
                article.source = feed.entries[i].title_detail.base
                article.description = re.sub(r"[^a-zA-Z0-9üäöÜÄÖß-]+", ' ', strip_tags(feed.entries[i].description))
                article.timeOfCrawl = strftime('%Y-%m-%d %H:%M:%S', gmtime())

                #article_guid = str(getGUID(article.host, feed.entries[i].guid))

                #Call method to request the article link and return the HTML
                parsedHtml = requestPage(article.link)

                if (article.host == "faz"):

                    #Parse GUID
                    regEx = re.search(r".net\W(.*)", feed.entries[i].guid)
                    article.guid = regEx[1]

                    #Parse Keywords if no keywords can be found the keyword is set to "NOT GIVEN"
                    try:
                        selectKeywordTag = parsedHtml.findAll("meta", {"name": "keywords"})
                        article.keywords = selectKeywordTag[0]['content']
                    except:
                        article.keywords = "NOT GIVEN"

                    #Parse category if no category can be found the category is set to "NOT GIVEN"
                    try:
                        selectCategoryTag = parsedHtml.findAll("div", {"class": "js-adobe-digital-data is-Invisible"})
                        result = selectCategoryTag[0]['data-digital-data']
                        infoJson = json.loads(result)
                        article.category = infoJson["page"]["ressort"]
                    except:
                        article.category = "NOT GIVEN"

                    #Parse author if no author can be found the author is set to "NOT GIVEN"
                    try:
                        results = parsedHtml.findAll("div", {"class": "js-adobe-digital-data is-Invisible"})
                        result = results[0]['data-digital-data']
                        infoJson = json.loads(result)
                        article.author = infoJson["article"]["author"]
                    except:
                        article.author = "NOT GIVEN"

                    #Parsing the text of the article
                    ts = article.timeOfCrawl.replace(" ", "")
                    selectedText = parsedHtml.findAll('p', {'class': 'atc-TextParagraph'})
                    extractedText = ""
                    for item in selectedText:
                        item = item.decode_contents()
                        extractedText += re.sub('<[^>]+>', '', item).replace('\n', '')
                    with open(f"{str(localPath)}/{d}_{str(article.host)}_{str(article.guid)}.txt",
                              "w", encoding="utf-8") as f:
                        f.write(extractedText)
                        f.close()

                elif (article.host == "stuttgarter-zeitung"):

                    #Parse GUID
                    regEx = re.search(r"-.*\.(.*?).html", feed.entries[i].guid)
                    article.guid = regEx[1]

                    #Parse Keywords if no keywords can be found the keyword is set to "NOT GIVEN"
                    try:
                        selectKeywordTag = parsedHtml.findAll("meta", {"name": "keywords"})
                        article.keywords = selectKeywordTag[0]['content']
                    except:
                        article.keywords = "NOT GIVEN"

                    #Parse category if no category can be found the category is set to "NOT GIVEN"
                    try:
                        regExCategory = re.findall(r"'pageRessort':\s'(.*?)'", str(parsedHtml))
                        article.category = regExCategory[0]
                    except:
                        article.category = "NOT GIVEN"

                    #Parse author if no author can be found the author is set to "NOT GIVEN"
                    try:
                        article.author = feed.entries[i].author
                    except:
                        article.author = "NOT GIVEN"

                    #Parsing the text of the article
                    ts = article.timeOfCrawl.replace(" ", "")
                    dpText = ""
                    selectedText = parsedHtml.findAll('div', {'class': 'brickgroup mod-article'})
                    stringSelectedText = str(selectedText)
                    deeperText = re.findall(r'<p>(.*)<\/p>', stringSelectedText)
                    for item in deeperText:
                        dpText += item

                    extractedText = re.sub('<[^>]+>', '', dpText).replace('processBricks();', '')

                    with open(f"{str(localPath)}/{d}_{str(article.host)}_{str(article.guid)}.txt",
                              "w", encoding="utf-8") as f:
                        f.write(extractedText)
                        f.close()
                elif (article.host == "sueddeutsche"):
                    # Parse GUID
                    article.guid = feed.entries[i].guid

                    #Parse Keywords if no keywords can be found the keyword is set to "NOT GIVEN"
                    try:
                        selectKeywordTag = parsedHtml.findAll("meta", {"name": "keywords"})
                        article.keywords = selectKeywordTag[0]['content']
                    except:
                        article.keywords = "NOT GIVEN"

                    #Parse category if no category can be found the category is set to "NOT GIVEN"
                    try:
                        selectCategoryTag = parsedHtml.findAll("script", {"type": "text/javascript"})
                        categoryTag = selectCategoryTag[0].decode_contents()
                        category = categoryTag.split("[")[1].split("]")[0]
                        categoryJson = json.loads(category)
                        article.category = categoryJson["ressort"]
                    except:
                        article.category = "NOT GIVEN"

                    #Parse author if no author can be found the author is set to "NOT GIVEN"
                    try:
                        article.author = feed.entries[i].author
                    except:
                        article.author = "NOT GIVEN"

                    #Parsing the text of the article
                    ts = article.timeOfCrawl.replace(" ", "")
                    selectedText = parsedHtml.findAll('div', {'class': 'sz-article__body sz-article-body'})
                    stringSelectedText = str(selectedText)
                    extractedText = re.sub('<[^>]+>', '', stringSelectedText).replace(
                        'AdController.render("iqadtile8")',
                        '').replace('\n', '')

                    with open(f"{str(localPath)}/{d}_{str(article.host)}_{str(article.guid)}.txt",
                              "w", encoding="utf-8") as f:
                        f.write(extractedText)
                        f.close()


                article.author = article.author.replace('Von ', '')

                sql_select_authors = "select author from author"
                cursor.execute(sql_select_authors)
                authorList = cursor.fetchall()
                formattedAuthorList = []
                for auth in authorList:
                    formattedAuthorList.append(auth[0])
                if article.author in formattedAuthorList:
                    sql_select_authornumber = "select authorID from author where author='{}'".format(article.author)
                    cursor.execute(sql_select_authornumber)
                    authorPosition = cursor.fetchall()
                    print("Found Author with the value " + str(article.author))
                else:
                    sql_insert_author = "insert author (author) values ('{}')".format(article.author)
                    cursor.execute(sql_insert_author)
                    connection.commit()
                    print(cursor.rowcount, "Record inserted successfully into Author table")
                    sql_select_authornumber = "select authorID from author where author='{}'".format(article.author)
                    cursor.execute(sql_select_authornumber)
                    authorPosition = cursor.fetchall()
                    print("Found no author with the value " + str(article.author))

                authorPosition = authorPosition[0][0]
                print("Author Position: " + str(authorPosition))

                sql_select_category = "select category from categories"
                cursor.execute(sql_select_category)
                categoryList = cursor.fetchall()
                formattedCategoryList = []
                for cat in categoryList:
                    formattedCategoryList.append(cat[0])
                if article.category in formattedCategoryList:
                    sql_select_categorynumber = "select categoryID from categories where category='{}'".format(article.category)
                    cursor.execute(sql_select_categorynumber)
                    categoryPosition = cursor.fetchall()
                    print("Found category with the value " + str(article.category))
                else:
                    sql_insert_category = "insert categories (category) values ('{}')".format(article.category)
                    cursor.execute(sql_insert_category)
                    connection.commit()
                    print(cursor.rowcount, "Record inserted successfully into Categories table")
                    sql_select_categorynumber = "select categoryID from categories where category='{}'".format(article.category)
                    cursor.execute(sql_select_categorynumber)
                    categoryPosition = cursor.fetchall()
                    print("Found no category with the value " + str(article.category))

                categoryPosition = categoryPosition[0][0]
                print("Category Position: " + str(categoryPosition))

                sql_select_newspaper = "select newspaper from newspaper"
                cursor.execute(sql_select_newspaper)
                newspaperList = cursor.fetchall()
                formattedNewspaperList = []
                for newsp in newspaperList:
                    formattedNewspaperList.append(newsp[0])
                if article.host in formattedNewspaperList:
                    sql_select_newspapernumber = "select newspaperID from newspaper where newspaper='{}'".format(article.host)
                    cursor.execute(sql_select_newspapernumber)
                    newspaperPosition = cursor.fetchall()
                    print("Found newspaper with the value " + str(article.host))
                else:
                    sql_insert_newspaper = "insert newspaper (newspaper) values ('{}')".format(article.host)
                    cursor.execute(sql_insert_newspaper)
                    connection.commit()
                    print(cursor.rowcount, "Record inserted successfully into Newspaper table")
                    sql_select_newspaperynumber = "select newspaperID from newspaper where newspaper='{}'".format(article.host)
                    cursor.execute(sql_select_newspaperynumber)
                    newspaperPosition = cursor.fetchall()
                    print("Found no newspaper with the value " + str(article.host))

                newspaperPosition = newspaperPosition[0][0]
                print("Newspaper Position: " + str(newspaperPosition))

                sql_select_keyword = "select keyword from keywords"
                cursor.execute(sql_select_keyword)
                keywordList = cursor.fetchall()
                formattedKeywordList = []
                for keyw in keywordList:
                    formattedKeywordList.append(keyw[0])

                keywords = article.keywords.split(",")
                keywordPositions = []
                for keywrd in keywords:
                    keywrd = keywrd.strip()
                    if keywrd in formattedKeywordList:
                        sql_select_keywordnumber = "select keywordID from keywords where keyword='{}'".format(keywrd)
                        cursor.execute(sql_select_keywordnumber)
                        keywordPosition = cursor.fetchall()
                        keywordPositions.append(keywordPosition[0][0])
                        print("Found keyword with the value " + str(keywrd))
                    else:
                        sql_insert_keyword = "insert keywords (keyword) values ('{}')".format(keywrd)
                        cursor.execute(sql_insert_keyword)
                        connection.commit()
                        print(cursor.rowcount, "Record inserted successfully into Keywords table")
                        sql_select_keywordnumber = "select keywordID from keywords where keyword='{}'".format(keywrd)
                        cursor.execute(sql_select_keywordnumber)
                        keywordPosition = cursor.fetchall()
                        keywordPositions.append(keywordPosition[0][0])
                        print("Found no keyword with the value " + str(keywrd))


                article.localLink = (str(localPath) + "/" + d + str(article.host) + "_" + str(article.guid) + ".txt")

                # article insert
                sql_insert_article = "insert article (authorID, newspaperID, title, description, source, external_link, local_link, time_of_publication, time_of_crawl) values ({}, {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(
                    authorPosition, newspaperPosition, article.title, article.description, article.source,
                    article.link, article.localLink, article.timeOfPublish, article.timeOfCrawl)
                cursor.execute(sql_insert_article)
                connection.commit()
                print(cursor.rowcount, "Record inserted successfully into Article table")
                articleID = cursor.lastrowid

                # article_categories insert
                sql_insert_article_categories = "insert article_categories (categoryID, articleID) VALUES ('{}', '{}')".format(
                    categoryPosition, articleID)
                cursor.execute(sql_insert_article_categories)
                connection.commit()
                print(cursor.rowcount, "Record inserted successfully into Article_Category table")

                # article_keywords insert
                for keywordP in keywordPositions:
                    sql_insert_article_keywords = "insert article_keywords (keywordID, articleID) VALUES ('{}', '{}')".format(
                        keywordP, articleID)
                    cursor.execute(sql_insert_article_keywords)
                    connection.commit()
                    print(cursor.rowcount, "Record inserted successfully into Article_Keyword table")


                # Output_CLI
                print("______________________________________________")
                print(f"Site:\t\t\t\t\t{article.host}")
                print(f"Article title:\t\t\t\t{article.title}")
                print(f"Source:\t\t\t\t\t{article.source}")
                print(f"Article description:\t\t\t{article.description}")
                print(f"Article external link:\t\t\t{article.link}")
                print(f"Article keyword:\t\t\t{article.keywords}")
                print(f"Article category:\t\t\t{article.category}")
                print(f"Article GuID:\t\t\t\t{article.guid}")
                print(f"Article local link:\t\t\t{article.localLink}")
                print(f"Timestamp of crawl:\t\t\t{article.timeOfCrawl}")
                print(f"Timestamp of publish:\t\t\t{article.timeOfPublish}")
                print(f"Author:\t\t\t\t\t{article.author}")
                print("______________________________________________")
                i += 1
    except Exception as ex:
        print("Following exception occured: " + str(ex))
        raise ex
    finally:
        if (connection.is_connected()):
            connection.close()
            cursor.close()
            print("MySQL connection is closed")


import pickle
from bs4 import BeautifulSoup
import re
from elasticsearch import Elasticsearch
import json
import os
import pathlib
import requests

inlinksDict = {}

def initEsInstance(esInstance, indexName, docTypeName):

    esInstance.indices.create(index=indexName, ignore=400)

    esInstance.indices.close(index=indexName)

    esInstance.indices.put_settings(index=indexName, body={
        "index" : {
                    "number_of_replicas": 2,
                    "analysis": {
                        "filter": {
                            "my_stemmer" : {
                                "type" : "stemmer",
                                "name" : "english"
                            }
                        },
                        "analyzer": {
                            "stopped": {
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": [
                                    "lowercase",
                                    "my_stemmer"
                                ]
                            }
                        }
                    }
                }
            })

    esInstance.indices.open(index = indexName)       

    esInstance.indices.put_mapping(index=indexName, doc_type=docTypeName, body={"properties": {
                    "head": {
                        "type": "text",
                        "fielddata": True, 
                        "analyzer": "stopped",
                        "index_options": "positions"
                    },
                    "text": {
                        "type": "text",
                        "fielddata": True, 
                        "analyzer": "stopped",
                        "index_options": "positions"
                    },
                    "inlinks": {
                        "type": "text",
                        "fielddata": True, 
                        "analyzer": "stopped"
                    },
                    "outlinks": {
                        "type": "text",
                        "fielddata": True, 
                        "analyzer": "stopped"
                    }
                }
            }, include_type_name = True)

def loadOutlinks():
    outlinksfile = open("Pickles/outlinks", "rb")
    outlinks = pickle.load(outlinksfile)
    outlinksfile.close()
    return outlinks

def loadInlinks():
    inlinksfile = open("Pickles/inlinks", "rb")
    inlinksDict = pickle.load(inlinksfile)
    inlinksfile.close()
    return inlinksDict    

def getAllDocs():
    docregex = re.compile('<DOC>.*?</DOC>', re.DOTALL)
    f = open("Files/content.txt", "r", encoding='ISO-8859-1')
    contents = f.read()
    documents = re.findall(docregex, contents)
    return documents

def getDocsAsXMLPages():
    fileX = open("Files/content.txt", "r", encoding='ISO-8859-1') 
    contentsX = fileX.read()
    data = "<head>" + contentsX + "</head>"
    soup = BeautifulSoup(data, features='xml')
    return soup.find_all('DOC')    

def getElementData(document, keytype):
    data = ""
    texts = document.find_all(keytype)
    for t in texts:
        data += t.get_text().strip() + ""
    return data

def dumpInlinks():
    inlinksfile = open("Pickles/inlinks", "wb")
    pickle.dump(inlinks_dict, inlinksfile)
    inlinksfile.close()

def saveProgress(input_file, data):
    temp_file = open("Pickles/" + input_file, "wb")
    pickle.dump(data, temp_file)
    temp_file.close()

def loadProgress(input_file):
    try:
        temp_file = open("Pickles/" + input_file, "rb")
    except FileNotFoundError:
        return None   
    m_data = pickle.load(temp_file)
    temp_file.close()
    return m_data

docno_regex = re.compile('<DOCNO>.*?</DOCNO>', re.DOTALL)
outlinks = loadOutlinks()
inlinks_dict = loadInlinks()

indexName = "ir_hw03_prod"
docTypeName = "hw03_crawl_contents"

esInstance1 = Elasticsearch("http://localhost:9200")
esInstance2 = Elasticsearch("https://96aa4157ead74b5ca4926523b1d1994e.us-east-1.aws.found.io:9243",
            http_auth=('elastic', 'MrkfJ5hxIcCOzTMfOa1Nftzy'))

initEsInstance(esInstance1, indexName, docTypeName)
initEsInstance(esInstance2, indexName, docTypeName)

documents = getAllDocs()
docCount = 1

docno_regex = re.compile('<DOCNO>.*?</DOCNO>', re.DOTALL)

for document in documents:
    docCount += 1
    inlinks = []
    doc_id = ''.join(re.findall(docno_regex, document)).replace('<DOCNO>', '').replace('</DOCNO>', '')
    if doc_id in inlinks_dict:
        continue
    print("Compute inlink for " + str(docCount))

    for key in outlinks:
        if doc_id in outlinks[key]:
            inlinks.append(key)
    inlinks_dict[doc_id] = inlinks
dumpInlinks()

docCount = 1
#addedIds = {}
addedIds = loadProgress("esWrite")

if addedIds == None:
    addedIds = dict()

title_regex = re.compile('<TITLE>.*?</TITLE>', re.DOTALL)
text_regex = re.compile('<TEXT>.*?</TEXT>', re.DOTALL)
#documentsXml = getDocsAsXMLPages()
for document in documents:

    docid = ''.join(re.findall(docno_regex, document)).replace('<DOCNO>', '').replace('</DOCNO>', '')
    docCount += 1
    if docid in addedIds or len(docid) >= 500:
        print("Skipping page : " + docid)
        addedIds[docid] = docid
        continue
    
    print("Indexing " + str(docCount) +  " docId: " + docid)

    addedIds[docid] = docid
    head = ''.join(re.findall(docno_regex, document)).replace('<TITLE>', '').replace('</TITLE>', '')
    text = ''.join(re.findall(docno_regex, document)).replace('<TEXT>', '').replace('</TEXT>', '')
    if text == '':
        print("No text for page: " + docid)
    inlinkData = inlinks_dict[doc_id]
    outlinkData = list(outlinks[doc_id])
    inlinkData = json.dumps(inlinkData)
    outlinkData = json.dumps(outlinkData)
    doc = {
        'head': head,
        'text': text,
        'inlinks': inlinkData,
        'outlinks': outlinkData
    }
    if docCount % 30 == 0:
        saveProgress("esWrite", addedIds)
        
    esInstance1.index(index=indexName, doc_type=docTypeName, id=docid, body = doc)
    esInstance2.index(index=indexName, doc_type=docTypeName, id=docid, body = doc)








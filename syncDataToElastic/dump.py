import pymysql
import json
import http.client
import datetime
import configparser

BULK_PATH_URL = "/%s/%s/_bulk?pretty&refresh"
headers = {'Content-Type': 'application/json'}

cf = configparser.ConfigParser()
cf.read("./config.ini")


def datetimeConverter(o):
  if isinstance(o, datetime.datetime):
    return int(o.timestamp()) * 1000

def construcIndex(ID):
  d = {}
  d["index"] = {"_id":str(ID)}
  return d

def getColumnNames(table_schema, table_name):
  connection = pymysql.connect(host=cf['mysql']['host'],user=cf['mysql']['user'],password=cf['mysql']['password'],database=cf['mysql']['database'])
  try:
    with connection.cursor() as cursor:
      sql = "SELECT column_name FROM information_schema.`COLUMNS` WHERE table_schema=%s and table_name=%s"
      cursor.execute(sql, (table_schema, table_name))
      result = cursor.fetchall()
      return [x[0] for x in result]
  finally:
    connection.close()

def getData(table_schema, table_name, table_columns, start, size):
  connection = pymysql.connect(host=cf['mysql']['host'],user=cf['mysql']['user'],password=cf['mysql']['password'],database=table_schema)
  try:
    with connection.cursor() as cursor:
      sql = "SELECT * FROM %s LIMIT %d,%d" % (table_name, start, size)
      cursor.execute(sql)
      result = cursor.fetchall()
      l = []
      for row in result:
        ID = row[0]
        l.append(json.dumps(construcIndex(ID), default=datetimeConverter))
        l.append(json.dumps({k:v for k,v in zip(table_columns, row)},ensure_ascii=False,default=datetimeConverter))
      return l
  finally:
    connection.close()

def syncToElasticsearch(indexName, typeName, data):
  connection = http.client.HTTPConnection(host=cf['elasticsearch']['host'],port=cf['elasticsearch']['port'])
  path = BULK_PATH_URL % (indexName, typeName)
  data = "\r\n".join(data)
  data += "\r\n" #最后一行必须有一个空行，这是ES的Rest接口规定的
  connection.request(method='POST', url=path , body=data.encode("utf-8"), headers=headers,encode_chunked=True)
  response = connection.getresponse()
  if (response.status == 200):
    print("sync %s data to elasticsearch success" % (typeName))
  connection.close()

def loadData(schemaName, tableName, indexName, typeName, start=0, size=10, limit=3000):
  columns = getColumnNames(table_schema=schemaName, table_name=tableName)


  while True: 
    data = getData(indexName,typeName,columns,start=start,size=size)
    if len(data) == 0 or start >= limit:
      print(typeName + " data load finished")
      break
    syncToElasticsearch(indexName,typeName, data)
    start += size



if __name__ == "__main__":

  dataCount = cf['data-info']['data-count']
  # load data
  for i in range(int(dataCount)):
    loadData(
      schemaName=cf['data-info']['data-schemaName[%d]' % (i)],
      tableName=cf['data-info']['data-tableName[%d]' % (i)],
      indexName=cf['data-info']['data-indexName[%d]' % (i)],
      typeName=cf['data-info']['data-typeName[%d]' % (i)]
    )


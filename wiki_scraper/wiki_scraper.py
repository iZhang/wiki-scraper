from urllib.request import urlopen
from bs4 import BeautifulSoup
import boto3

def scraper(event,context):
	try:
		url = "https://en.wikipedia.org/wiki/List_of_virtual_communities_with_more_than_100_million_active_users"		
		html = urlopen(url)
		soup = BeautifulSoup(html, 'html.parser')
		wiki_table = soup.find("table", {"class":"wikitable sortable"})
		raw_data = []
		parsed_data = {}

		dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://dynamodb.us-east-1.amazonaws.com")
		dynamo_table = dynamodb.Table('PRODUCT_METRICS')

		table_body = wiki_table.find('tbody')
		rows = table_body.find_all('tr')

		for row in rows:
		    cols = row.find_all('td')
		    cols = [ele.text.strip() for ele in cols]
		    raw_data.append([ele for ele in cols if ele]) # Get rid of empty values

		for i in raw_data[1:]:
			response = dynamo_table.put_item(
		   		Item =
		   		{
			        "RANKING": i[0],
			        "PRODUCT_ID": i[1],
			        "PARENT_COMPANY": i[2],
			        "USER_COUNT": i[3][:-4],
			        "PRODUCT_RELEASE_DATE": i[4],
			        "COUNTRY_OF_ORIGIN": i[5],
			        "DATE_LAST_UPDATED": i[6]
		    	}
			)
			print("PutItem succeeded")	
	
	except Exception as e:
            print (e)
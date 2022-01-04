from kafka import KafkaProducer
import requests
import time
from bs4 import BeautifulSoup
from datetime import timedelta, date

producer = KafkaProducer(bootstrap_servers='masternode:9092')

url_base = 'https://coinmarketcap.com/historical/'
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

start_date = date(2021, 12, 30)
end_date = date(2022, 1, 1)

def runaround(single_date):
    time.sleep(20)
    url = url_base + single_date.strftime("%Y%m%d")
    print(url)
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    return soup.find('script', type='application/json')

for single_date in daterange(start_date, end_date):
    url = url_base + single_date.strftime("%Y%m%d")
    print(url)
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    s = soup.find('script', type='application/json')
    while s is None:
        s = runaround(single_date)
    s=s.string
    start = s.find('[{"id":1,')
    s=s[start:]
    end = s.find(',"page":1')
    s=s[:end]
    producer.send('hdsd', bytes(s,'utf-8'))
    producer.flush()
    time.sleep(5)
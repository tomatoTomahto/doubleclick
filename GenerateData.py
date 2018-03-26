import random, csv, os
from faker import Faker
from tqdm import tqdm
import google.cloud.storage

CLIENTS = 50
MIN_IMPRESSIONS = 100
MAX_IMPRESSIONS = 500
DAYS_HISTORY = 0
MAX_USERS = 1000000
MAX_ADVERTISERS = 5
MAX_LINE_ITEMS = 100
MAX_AUD_SEGMENTS = 100

fake = Faker()

storage_client = google.cloud.storage.Client()
bucket_name = 'sgupta_doubleclick'
bucket = storage_client.get_bucket(bucket_name)

for clientID in tqdm(range(CLIENTS)):
  clientIDStr = '%04d'%clientID
  for dateOffset in range(-DAYS_HISTORY,1):
    dataDate = fake.date_between(start_date="-%dd"%-dateOffset, end_date="+%dd"%dateOffset)
    dataDateStr = dataDate.strftime('%Y-%m-%d')

    impressionFile = os.path.join('staging',clientIDStr,dataDateStr,'impressions.csv')    
    impressions = []
    impressionCount = random.randint(MIN_IMPRESSIONS,MAX_IMPRESSIONS)
    for impression in range(impressionCount):
      viewable = random.randint(0,10)>2
      impressions.append({'impressionID' : '%010d'%impression,
        'time' : fake.date_time_between(start_date="%dd"%(dateOffset-1), end_date="+%dd"%(dateOffset)),
        'userID' : random.randint(1, MAX_USERS),
        'advertiserID' : random.randint(1,MAX_ADVERTISERS),
        'lineItemID' : random.randint(1,MAX_LINE_ITEMS),
        'viewable' : viewable,
        'audienceSegmentID' : random.randint(1,MAX_AUD_SEGMENTS),
        'duration' : fake.random_int(min=1,max=10) if viewable else 0,
        'domain' : fake.domain_name(),
        'country' : fake.country(),
        'city' : fake.city(),
        'postalCode' : fake.postalcode(),
        'eventType' : fake.word(ext_word_list=['click','view']) if viewable else 'VIEW',
        'browser' : fake.word(ext_word_list=['chrome','safari','ie','edge','firefox']),
        'os' : fake.word(ext_word_list=['andriod','ios','windows','macos']),
        'mobileDevice' : fake.word(ext_word_list=['samsung','apple','microsoft','lg']),
        'carrier' : fake.word(ext_word_list=['at&t','tmobile','sprint']),
        'sellerPrice' : fake.random_int(min=1,max=100)})

    matchFile = os.path.join('staging',clientIDStr,dataDateStr,'match.csv')
    matches = []
    for advertiserID in range(1,MAX_ADVERTISERS):
      matches.append({'matchType' : 'advertiser',
                      'matchID' : advertiserID,
                      'matchName' : '%s'%fake.company()})
    
    for audienceSegmentID in range(1,MAX_AUD_SEGMENTS):
      gender = fake.word(ext_word_list=['M','F'])
      age = fake.random_int(min=1,max=50)
      descriptor = fake.word(ext_word_list=['sports enthusiast','business mind',
                                            'scientific','academic','thrifty',
                                            'tech savvy','family oriented',
                                            'health nut','socially butterfly'])
      segment = '%s_%d_%s' % (gender, age, descriptor)
      matches.append({'matchType' : 'audience_segment',
                      'matchID' : audienceSegmentID,
                      'matchName' : segment})
      
    for lineItemID in range(1,MAX_LINE_ITEMS):
      size = fake.word(ext_word_list=['200x200','300x300','400x400','500x500'])
      location = fake.word(ext_word_list=['bottom left','bottom right','bottom center',
                                          'top left', 'top right','top center'])
      lineItem = '%s_%s' % (size, location)
      matches.append({'matchType' : 'line_item',
                      'matchID' : lineItemID,
                      'matchName' : lineItem})
      
    if not os.path.exists(os.path.dirname(impressionFile)):
      try:
          os.makedirs(os.path.dirname(impressionFile))
      except OSError as exc: # Guard against race condition
          if exc.errno != errno.EEXIST:
              raise

    with open(impressionFile,'wb') as f:
      dw = csv.DictWriter(f,impressions[0].keys())
      dw.writeheader()
      dw.writerows(impressions)
      
    if not os.path.exists(os.path.dirname(matchFile)):
      try:
          os.makedirs(os.path.dirname(matchFile))
      except OSError as exc: # Guard against race condition
          if exc.errno != errno.EEXIST:
              raise

    with open(matchFile,'wb') as f:
      dw = csv.DictWriter(f,matches[0].keys())
      dw.writeheader()
      dw.writerows(matches)
      
    impressionBlob = bucket.blob(impressionFile)
    impressionBlob.upload_from_filename(impressionFile)
    matchBlob = bucket.blob(matchFile)
    matchBlob.upload_from_filename(matchFile)
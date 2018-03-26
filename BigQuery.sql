# Create a view on only Client 1's Data
SELECT * FROM [DoubleClick.impressions] WHERE clientName = "Client 1";

# Top device and browser combinations on Android leading to the most views
SELECT mobileDevice, browser, count(*) as impressions, sum(case when eventType='click' then 1 else 0 end) AS clicks 
FROM [DoubleClick.client1_impressions] 
WHERE os = 'andriod'
GROUP BY mobileDevice, browser
ORDER BY clicks DESC
LIMIT 5;

# Cost and Impressions by Advertiser by Day
SELECT DATE(time) as date, advertiser, sum(sellerPrice) as totalCost, count(*) as totalImpressions
FROM [DoubleClick.client1_impressions] 
GROUP BY date, advertiser
ORDER BY date DESC, advertiser DESC;
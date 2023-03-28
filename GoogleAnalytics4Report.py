import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class GoogleAnalytics4Report:
    def __init__(self, api_key_file, property_id, start_date, end_date, dimensions, metrics):
        self.api_key_file = api_key_file
        self.property_id = property_id
        self.start_date = start_date
        self.end_date = end_date
        self.dimensions = dimensions
        self.metrics = metrics

    def get_report(self):
        try:
            credentials = service_account.Credentials.from_service_account_file(self.api_key_file)
            analytics = build('analyticsreporting', 'v4', credentials=credentials)

            report_request = {
                'viewId': self.property_id,
                'dateRanges': [{'startDate': self.start_date, 'endDate': self.end_date}],
                'dimensions': [{'name': dim} for dim in self.dimensions],
                'metrics': [{'expression': metric} for metric in self.metrics]
            }

            response = analytics.reports().batchGet(body={'reportRequests': [report_request]}).execute()
            report = response['reports'][0]

            column_header = report['columnHeader']['dimensions'] + [metric['alias'] for metric in report['columnHeader']['metricHeader']['metricHeaderEntries']]
            data_rows = []
            for row in report['data']['rows']:
                data_row = row['dimensions'] + [metric['values'][0] for metric in row['metrics']]
                data_rows.append(data_row)

            df = pd.DataFrame(data_rows, columns=column_header)

            return df

        except HttpError as error:
            print('There was an error querying the GA API: {0}'.format(error))

    def to_csv(self, file_name):
        df = self.get_report()
        df.to_csv(file_name, index=False)


# Example usage:
ga = GoogleAnalytics4Report(api_key_file='<API-KEY_FILE.json>',
                      property_id='<GA4-PROPERTY-ID>',
                      start_date='2023-03-01',
                      end_date='2023-03-21',
                      dimensions=['browser', 'dataSource', 'date', 'deviceCategory', 'language', 'platform', 'propertyName', 'streamName'],
                      metrics=['28dayUsers', '7dayUsers', 'activeUsers', 'addCart', 'checkouts', 'totalConversions', 'totalPurchases', 'engagementRate', 'eventCount', 'eventValue', 'firstTimePurchasers', 'itemListClicks', 'itemListViews', 'itemPromotionClicks', 'itemPromotionViews', 'itemViews', 'newUsers', 'pageviews', 'sessions', 'adRevenue', 'totalUsers', 'transactionRevenue', 'transactions', 'userEngagementDuration'])

ga.to_csv('ga4_data.csv')

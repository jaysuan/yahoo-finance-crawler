import logging

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from scrapy.exporters import BaseItemExporter

logger = logging.getLogger(__name__)

class GoogleSheetItemExporter(BaseItemExporter):
    def __init__(self, spreadsheet, worksheet):
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name('google_client_secret.json', scope)
        self.client = gspread.authorize(self.credentials)
        self.spreadsheet = self.client.open(spreadsheet)
        self.worksheet = self.spreadsheet.worksheet(worksheet)

    def export_item(self, item):
        q4_net_income_perc = item['q4_net_income_percentage']
        company = item['company_name']
        logger.debug(f"Q4 Net income percentage of {company}: {q4_net_income_perc}")
        if self.credentials.access_token_expired:
            self.client.login()
        self.worksheet.append_row([
            item['date'],
            item['ticker'],
            item['company_name'],
            item['previous_close'],
            item['pe_ratio'],
            item['fiftytwo_week_high'],
            item['diff_to_52_week_high'],
            item['fair_value'],
            item['one_year_target_est'],
            item['diff_to_1y_target_est'],
            item['forward_pe'],
            item['ttm_net_income_percentage'],
            item['q1_net_income_percentage'],
            item['q2_net_income_percentage'],
            item['q3_net_income_percentage'],
            item['q4_net_income_percentage'],
            item['balance_sheet']['Q1']['net_tangible_assets'],
            item['balance_sheet']['Q1']['debt_to_equity_ratio'],
            item['net_change_in_cash'],
            item['free_cash_flow'],
            item['market_cap'],
            item['peg_ratio'],
            item['price_over_sales'],
            item['price_over_book'],
            item['return_on_assets'],
            item['return_on_equity'],
            item['diluted_eps'],
            item['quarterly_earnings_growth'],
            item['fwd_annual_dividend_rate'],
            item['fwd_annual_dividend_yield'],
            item['ex_dividend_date'],
            item['vanguard_holder'],
            item['country']
        ])
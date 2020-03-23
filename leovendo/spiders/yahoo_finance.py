import locale
import re
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from scrapy import Request, Spider
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


from leovendo.http import SeleniumRequest
from leovendo.items import Component

class YahooFinanceSpider(Spider):
    name = 'yahoo_finance'
    base_url = 'https://finance.yahoo.com'
    allowed_domains = ['finance.yahoo.com']
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

    def start_requests(self):
        tickers = self.settings.get('TICKERS')
        self.logger.debug(f"Tickers settings: {tickers}")
        for ticker in tickers:
            url = f"{self.base_url}/quote/{ticker}?p={ticker}"
            nav_urls = {
                "statistics_url": f"{self.base_url}/quote/{ticker}/key-statistics?p={ticker}",
                "profile_url": f"{self.base_url}/quote/{ticker}/profile?p={ticker}",
                "holders_url": f"{self.base_url}/quote/{ticker}/holders?p={ticker}",
                "financials_url": f"{self.base_url}/quote/{ticker}/financials?p={ticker}",
                "balance_sheet_url": f"{self.base_url}/quote/{ticker}/balance-sheet?p={ticker}",
                "cash_flow_url": f"{self.base_url}/quote/{ticker}/cash-flow?p={ticker}"
            }
            yield SeleniumRequest(
                url=url,
                callback=self.parse_summary,
                cb_kwargs= { "ticker": ticker },
                spawn_driver=True,
                meta = { "nav_urls": nav_urls }
            )

    def parse_summary(self, response, ticker):
        nav_urls = response.meta['nav_urls']
        loader = ItemLoader(item=Component(), response=response)
        component_name = response.css("div#quote-header-info h1::text").get()
        self.logger.debug(f"Component name: {component_name}")
        if component_name:
            match = re.match(r'(.+\S)\s?\((.+)\)', component_name)
            loader.add_value('company_name', match.group(1))
        else:
            loader.add_value('company_name', '')
        loader.add_value('ticker', ticker)
        loader.add_value('date', datetime.today().strftime('%d/%m/%Y'))
        loader.add_css('previous_close', "[data-test=PREV_CLOSE-value] > span::text")
        loader.add_css('pe_ratio', '[data-test=PE_RATIO-value] > span::text')
        fair_value = response.xpath("//div[span[h5[contains(text(), 'Fair Value')]]]/following-sibling::div[1]/div[2]/text()").get()
        fair_value = fair_value if not None else 'N/A'
        loader.add_value('fair_value', fair_value)
        loader.add_css('one_year_target_est', "[data-test=ONE_YEAR_TARGET_PRICE-value] > span::text")
        
        statistics_url = nav_urls['statistics_url']
        yield SeleniumRequest(
            url=statistics_url,
            callback=self.parse_statistics,
            previous_response=response,
            meta={
                "loader": loader,
                "nav_urls": nav_urls
            }
        )
    
    def parse_statistics(self, response):
        driver = response.meta['driver']
        nav_urls = response.meta['nav_urls']
        parent_loader = response.meta['loader']
        loader = ItemLoader(parent=parent_loader, response=response)
        fiftytwo_week_high = response.xpath("//tr/td/span[text()='52 Week High']/parent::td/following-sibling::td[1]/text()").get()
        loader.add_value('fiftytwo_week_high', fiftytwo_week_high)
        previous_close = locale.atof(loader.get_output_value('previous_close'))
        one_year_target_est = locale.atof(loader.get_output_value('one_year_target_est'))
        diff_to_52_week_high = 1 - (previous_close - locale.atof(fiftytwo_week_high))
        diff_to_1y_target_est = 1 - (one_year_target_est - previous_close)
        loader.add_value('diff_to_52_week_high', f"{self._round_off_2_decimal(diff_to_52_week_high)}%")
        loader.add_value('diff_to_1y_target_est', f"{self._round_off_2_decimal(diff_to_1y_target_est)}%")

        forward_pe = self._wait_and_find_elem(driver, "//tr/td/span[text()='Forward P/E']/parent::td/following-sibling::td[1]").text
        loader.add_xpath('forward_pe', forward_pe)
        market_cap = response.xpath("//tr/td/span[contains(text(), 'Market Cap')]/parent::td/following-sibling::td[1]/text()").get()
        unit = market_cap[-1]
        if unit == 'B':
            multiplier = 1000
        elif unit == 'T':
            multiplier = 1000000
        else:
            multiplier = 1
        market_cap = float(market_cap[0:-1]) * multiplier
        loader.add_value('market_cap', market_cap)
        peg_ratio = response.xpath("//tr/td/span[contains(text(), 'PEG Ratio')]/parent::td/following-sibling::td[1]/text()").get() or \
            response.xpath("//tr/td/span[contains(text(), 'PEG Ratio')]/parent::td/following-sibling::td[1]/span/text()").get()
        loader.add_value('peg_ratio', peg_ratio)
        loader.add_xpath('price_over_sales', "//tr/td/span[contains(text(), 'Price/Sales')]/parent::td/following-sibling::td[1]/text()")
        price_over_book = response.xpath("//tr/td/span[contains(text(), 'Price/Book')]/parent::td/following-sibling::td[1]/text()").get() or \
            response.xpath("//tr/td/span[contains(text(), 'Price/Book')]/parent::td/following-sibling::td[1]/span/text()").get()
        loader.add_value('price_over_book', price_over_book)
        return_on_assets = response.xpath("//tr/td/span[contains(text(), 'Return on Assets')]/parent::td/following-sibling::td[1]/text()").get() or \
            response.xpath("//tr/td/span[contains(text(), 'Return on Assets')]/parent::td/following-sibling::td[1]/span/text()").get()
        loader.add_value('return_on_assets', return_on_assets)
        return_on_equity = response.xpath("//tr/td/span[contains(text(), 'Return on Equity')]/parent::td/following-sibling::td[1]/text()").get() or \
            response.xpath("//tr/td/span[contains(text(), 'Return on Equity')]/parent::td/following-sibling::td[1]/span/text()").get()
        loader.add_value('return_on_equity', return_on_equity)
        loader.add_xpath('diluted_eps', "//tr/td/span[contains(text(), 'Diluted EPS')]/parent::td/following-sibling::td[1]/text()")
        quarterly_earnings_growth = response.xpath("//tr/td/span[contains(text(), 'Quarterly Earnings Growth')]/parent::td/following-sibling::td[1]/text()").get() or \
            response.xpath("//tr/td/span[contains(text(), 'Quarterly Earnings Growth')]/parent::td/following-sibling::td[1]/span/text()").get()
        loader.add_value('quarterly_earnings_growth', quarterly_earnings_growth)
        fwd_annual_dividend_rate = response.xpath("//tr/td/span[contains(text(), 'Forward Annual Dividend Rate')]/parent::td/following-sibling::td[1]/text()").get() or \
            response.xpath("//tr/td/span[contains(text(), 'Forward Annual Dividend Rate')]/parent::td/following-sibling::td[1]/span/text()").get()
        loader.add_value('fwd_annual_dividend_rate', fwd_annual_dividend_rate)
        fwd_annual_dividend_yield = response.xpath("//tr/td/span[contains(text(), 'Forward Annual Dividend Yield')]/parent::td/following-sibling::td[1]/text()").get() or \
            response.xpath("//tr/td/span[contains(text(), 'Forward Annual Dividend Yield')]/parent::td/following-sibling::td[1]/span/text()").get()
        loader.add_value('fwd_annual_dividend_yield', fwd_annual_dividend_yield)
        ex_dividend_date = response.xpath("//tr/td/span[contains(text(), 'Ex-Dividend Date')]/parent::td/following-sibling::td[1]/text()").get() or \
            response.xpath("//tr/td/span[contains(text(), 'Ex-Dividend Date')]/parent::td/following-sibling::td[1]/span/text()").get()
        loader.add_value('ex_dividend_date', ex_dividend_date)
        
        yield SeleniumRequest(
            url=nav_urls['profile_url'],
            callback=self.parse_profile,
            previous_response=response,
            meta={
                "loader": loader,
                "nav_urls": nav_urls
            }
        )

    def parse_profile(self, response):
        nav_urls = response.meta['nav_urls']
        parent_loader = response.meta['loader']
        loader = ItemLoader(parent=parent_loader, response=response)
        address = response.xpath("//div[@data-test='asset-profile']/div/div/p/text()").extract()
        filtered = [addr for addr in address if addr != ':\xa0']
        loader.add_value('country', filtered[-1])
        yield SeleniumRequest(
            url=nav_urls['holders_url'],
            callback=self.parse_holders,
            previous_response=response,
            meta={
                "loader": loader,
                "nav_urls": nav_urls
            }
        )

    def parse_holders(self, response):
        nav_urls = response.meta['nav_urls']
        parent_loader = response.meta['loader']
        loader = ItemLoader(parent=parent_loader, response=response)
        vanguard_node = response.xpath("//h3/span[contains(text(), 'Top Institutional Holders')]/parent::h3/following-sibling::table/tbody/tr[1]//td[1][contains(text(), 'Vanguard')]")
        vanguard_holder = 'Y' if vanguard_node else 'N/A'
        loader.add_value('vanguard_holder', vanguard_holder)

        yield SeleniumRequest(
            url=nav_urls['financials_url'],
            callback=self.parse_income_statement,
            previous_response=response,
            meta={
                "loader": loader,
                "nav_urls": nav_urls
            }
        )

    def parse_income_statement(self, response):
        driver = response.meta['driver']
        nav_urls = response.meta['nav_urls']
        parent_loader = response.meta['loader']
        loader = ItemLoader(parent=parent_loader, response=response)

        self.logger.debug(f"Current URL: {driver.current_url}")
        quarterly_btn = driver.find_element_by_xpath("//button[div[span[text()='Quarterly']]]")
        quarterly_btn.click()

        q1_date = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//div[span[contains(text(), 'Breakdown')]]/following-sibling::div[2]/span"))
        ).text

        income_statement = {
            "TTM": {
                "date": driver.find_element_by_xpath("//div[span[contains(text(), 'Breakdown')]]/following-sibling::div[1]/span").text,
                "total_revenue": driver.find_element_by_xpath("//div[@title='Total Revenue']/parent::div/following-sibling::div[1]/span").text,
                "net_income": driver.find_element_by_xpath("//div[@title='Net Income']/parent::div/following-sibling::div[1]/span").text
            },
            "Q1": {
                "date": q1_date,
                "total_revenue": driver.find_element_by_xpath("//div[@title='Total Revenue']/parent::div/following-sibling::div[2]/span").text,
                "net_income": driver.find_element_by_xpath("//div[@title='Net Income']/parent::div/following-sibling::div[2]/span").text
            },
            "Q2": {
                "date": driver.find_element_by_xpath("//div[span[contains(text(), 'Breakdown')]]/following-sibling::div[3]/span").text,
                "total_revenue": driver.find_element_by_xpath("//div[@title='Total Revenue']/parent::div/following-sibling::div[3]/span").text,
                "net_income": driver.find_element_by_xpath("//div[@title='Net Income']/parent::div/following-sibling::div[3]/span").text
            },
            "Q3": {
                "date": driver.find_element_by_xpath("//div[span[contains(text(), 'Breakdown')]]/following-sibling::div[4]/span").text,
                "total_revenue": driver.find_element_by_xpath("//div[@title='Total Revenue']/parent::div/following-sibling::div[4]/span").text,
                "net_income": driver.find_element_by_xpath("//div[@title='Net Income']/parent::div/following-sibling::div[4]/span").text
            },
            "Q4": {
                "date": driver.find_element_by_xpath("//div[span[contains(text(), 'Breakdown')]]/following-sibling::div[5]/span").text,
                "total_revenue": driver.find_element_by_xpath("//div[@title='Total Revenue']/parent::div/following-sibling::div[5]/span").text,
                "net_income": driver.find_element_by_xpath("//div[@title='Net Income']/parent::div/following-sibling::div[5]/span").text
            },
        }
        for key in income_statement.keys():
            self.logger.debug(f"Current key: {key}")
            income_statement[key]['net_income_percentage'] = \
                locale.atoi(income_statement[key]['net_income']) / locale.atoi(income_statement[key]['total_revenue'])
            income_statement[key]['net_income_percentage'] = f"{self._round_off_2_decimal(income_statement[key]['net_income_percentage'])}%"
        # loader.add_value('income_statement', income_statement)
        loader.add_value('ttm_net_income_percentage', income_statement['TTM']['net_income_percentage'])
        loader.add_value('q1_net_income_percentage', income_statement['Q1']['net_income_percentage'])
        loader.add_value('q2_net_income_percentage', income_statement['Q2']['net_income_percentage'])
        loader.add_value('q3_net_income_percentage', income_statement['Q3']['net_income_percentage'])
        loader.add_value('q4_net_income_percentage', income_statement['Q4']['net_income_percentage'])
        yield SeleniumRequest(
            url=nav_urls['balance_sheet_url'],
            callback=self.parse_balance_sheet,
            previous_response=response,
            meta={
                "loader": loader,
                "nav_urls": nav_urls
            }
        )

    def parse_balance_sheet(self, response):
        driver = response.meta['driver']
        nav_urls = response.meta['nav_urls']
        parent_loader = response.meta['loader']
        loader = ItemLoader(parent=parent_loader, response=response)

        quarterly_btn = driver.find_element_by_xpath("//button[div[span[text()='Quarterly']]]")
        quarterly_btn.click()

        q1_date = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//div[span[contains(text(), 'Breakdown')]]/following-sibling::div[1]/span"))
        ).text
        balance_sheet = {
            "Q1": {
                "date": q1_date,
                "total_assets": driver.find_element_by_xpath("//div[@title='Total Assets']/parent::div/following-sibling::div[1]/span").text,
                "total_liabilities": driver.find_element_by_xpath("//div[@title='Total Liabilities']/parent::div/following-sibling::div[1]/span").text,
                "total_stockholders_equity": driver.find_element_by_xpath("//div[@title=\"Total stockholders' equity\"]/parent::div/following-sibling::div[1]/span").text
            },
            "Q2": {
                "date": driver.find_element_by_xpath("//div[span[contains(text(), 'Breakdown')]]/following-sibling::div[2]/span").text,
                "total_assets": driver.find_element_by_xpath("//div[@title='Total Assets']/parent::div/following-sibling::div[2]/span").text,
                "total_liabilities": driver.find_element_by_xpath("//div[@title='Total Liabilities']/parent::div/following-sibling::div[2]/span").text,
                "total_stockholders_equity": driver.find_element_by_xpath("//div[@title=\"Total stockholders' equity\"]/parent::div/following-sibling::div[2]/span").text
            },
            "Q3": {
                "date": driver.find_element_by_xpath("//div[span[contains(text(), 'Breakdown')]]/following-sibling::div[3]/span").text,
                "total_assets": driver.find_element_by_xpath("//div[@title='Total Assets']/parent::div/following-sibling::div[3]/span").text,
                "total_liabilities": driver.find_element_by_xpath("//div[@title='Total Liabilities']/parent::div/following-sibling::div[3]/span").text,
                "total_stockholders_equity": driver.find_element_by_xpath("//div[@title=\"Total stockholders' equity\"]/parent::div/following-sibling::div[3]/span").text
            },
            "Q4": {
                "date": driver.find_element_by_xpath("//div[span[contains(text(), 'Breakdown')]]/following-sibling::div[4]/span").text,
                "total_assets": driver.find_element_by_xpath("//div[@title='Total Assets']/parent::div/following-sibling::div[4]/span").text,
                "total_liabilities": driver.find_element_by_xpath("//div[@title='Total Liabilities']/parent::div/following-sibling::div[4]/span").text,
                "total_stockholders_equity": driver.find_element_by_xpath("//div[@title=\"Total stockholders' equity\"]/parent::div/following-sibling::div[4]/span").text
            }
        }
        for key in balance_sheet.keys():
            total_assets = locale.atoi(balance_sheet[key]['total_assets'])
            total_liabilities = locale.atoi(balance_sheet[key]['total_liabilities'])
            total_stockholders_equity = locale.atoi(balance_sheet[key]['total_stockholders_equity'])
            balance_sheet[key]['net_tangible_assets'] = locale.format("%d", (total_assets - total_liabilities), grouping=True)
            balance_sheet[key]['debt_to_equity_ratio'] = f"{self._round_off_2_decimal(total_liabilities / total_stockholders_equity)}%"
        loader.add_value('balance_sheet', balance_sheet)
        yield SeleniumRequest(
            url=nav_urls['cash_flow_url'],
            callback=self.parse_cash_flow,
            previous_response=response,
            meta={
                "loader": loader
            }
        )

    def parse_cash_flow(self, response):
        parent_loader = response.meta['loader']
        response.meta['driver'].quit()
        loader = ItemLoader(parent=parent_loader, response=response)
        net_change_in_cash = response.xpath("//div[div[@title='Net change in cash']]/following-sibling::div[1]/span/text()").get()
        free_cash_flow = response.xpath("//div[div[@title='Free Cash Flow']]/following-sibling::div[1]/span/text()").get()
        loader.add_value('net_change_in_cash', net_change_in_cash if net_change_in_cash else 'N/A')
        loader.add_value('free_cash_flow', free_cash_flow if free_cash_flow else 'N/A')
        return loader.load_item()

    def _wait_and_find_elem(self, driver, xpath, wait_time=20):
        return WebDriverWait(driver, wait_time).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )

    def _round_off_2_decimal(self, num):
        return Decimal(num).quantize(Decimal('.01'), rounding='ROUND_UP')

    def _save_screenshot(self, screenshot):
        Path('screenshots').mkdir(exist_ok=True)
        with open(f'screenshots/ss_{datetime.today().strftime("%m-%d-%Y_%H:%M:%S.%f")}.png', 'wb') as ss_file:
            ss_file.write(screenshot)

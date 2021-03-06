from scrapy import Field, Item
from scrapy.loader.processors import TakeFirst

class Component(Item):
    date = Field(output_processor=TakeFirst())
    ticker = Field(output_processor=TakeFirst())
    company_name = Field(output_processor=TakeFirst())
    previous_close = Field(output_processor=TakeFirst())
    pe_ratio = Field(output_processor=TakeFirst())
    fiftytwo_week_high = Field(output_processor=TakeFirst())
    diff_to_52_week_high = Field(output_processor=TakeFirst())
    fair_value = Field(output_processor=TakeFirst())
    forward_pe = Field(output_processor=TakeFirst())
    one_year_target_est = Field(output_processor=TakeFirst())
    diff_to_1y_target_est = Field(output_processor=TakeFirst())
    market_cap = Field(output_processor=TakeFirst())
    peg_ratio = Field(output_processor=TakeFirst())
    price_over_sales = Field(output_processor=TakeFirst())
    price_over_book = Field(output_processor=TakeFirst())
    return_on_assets = Field(output_processor=TakeFirst())
    return_on_equity = Field(output_processor=TakeFirst())
    diluted_eps = Field(output_processor=TakeFirst())
    quarterly_earnings_growth = Field(output_processor=TakeFirst())
    fwd_annual_dividend_rate = Field(output_processor=TakeFirst())
    fwd_annual_dividend_yield = Field(output_processor=TakeFirst())
    ex_dividend_date = Field(output_processor=TakeFirst())
    country = Field(output_processor=TakeFirst())
    vanguard_holder = Field(output_processor=TakeFirst())
    ttm_net_income_percentage = Field(output_processor=TakeFirst())
    q1_net_income_percentage = Field(output_processor=TakeFirst())
    q2_net_income_percentage = Field(output_processor=TakeFirst())
    q4_net_income_percentage = Field(output_processor=TakeFirst())
    q3_net_income_percentage = Field(output_processor=TakeFirst())
    balance_sheet = Field(output_processor=TakeFirst())
    net_change_in_cash = Field(output_processor=TakeFirst())
    free_cash_flow = Field(output_processor=TakeFirst())
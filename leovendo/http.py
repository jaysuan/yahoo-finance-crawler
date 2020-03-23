"""This module contains the ``SeleniumRequest`` class"""

from scrapy import Request


class SeleniumRequest(Request):
    """Scrapy ``Request`` subclass providing additional arguments"""

    def __init__(self, previous_response=None, wait_time=None, wait_until=None, screenshot=False, script=None,
        spawn_driver=False, *args, **kwargs):
        """Initialize a new selenium request

        Parameters
        ----------
        wait_time: int
            The number of seconds to wait.
        wait_until: method
            One of the "selenium.webdriver.support.expected_conditions". The response
            will be returned until the given condition is fulfilled.
        screenshot: bool
            If True, a screenshot of the page will be taken and the data of the screenshot
            will be returned in the response "meta" attribute.
        script: str
            JavaScript code to execute.

        """

        self.wait_time = wait_time
        self.wait_until = wait_until
        self.screenshot = screenshot
        self.script = script
        self.spawn_driver = spawn_driver
        if previous_response:
            self.driver = previous_response.meta['driver']
        else:
            self.driver = None

        super().__init__(*args, **kwargs)

    
import random
import string
from typing import Optional, Union

import requests
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.exceptions import NotConfigured
from scrapy.http.request import Request
from scrapy.spiders import Spider
from scrapy.utils.python import global_object_name
from scrapy.utils.response import response_status_message


class BaiduAOIMiddleware(RetryMiddleware):
    EXCEPTIONS_TO_RETRY = (
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.HTTPError,
        requests.exceptions.RequestException,
    )

    def __init__(self, settings):
        super().__init__(settings)
        self.proxy_pool_url = "http://127.0.0.1:5010"

    def get_proxy(self) -> str:
        """
        proxy pool is built with reference to https://github.com/jhao104/proxy_pool
        """
        try:
            proxy = requests.get(f"{self.proxy_pool_url}/get/").json()
            return f'http://{proxy["proxy"]}'
        except Exception as e:
            logging.error(f"Failed to get proxy: {str(e)}")
            return None

    def delete_proxy(self, proxy) -> None:
        try:
            requests.get(f"{self.proxy_pool_url}/delete/?proxy={proxy}")
        except Exception as e:
            logging.error(f"Failed to delete proxy {proxy}: {str(e)}")

    def get_cookie(self) -> str:
        """
        It is observed that `BAIDUID` cookie value
        is made up of 32 random numbers and letters.
        """

        def random_32_string():
            return "".join(
                random.choice(string.ascii_uppercase[:6] + string.digits)
                for _ in range(32)
            )

        bd_id = random_32_string()
        return f"{bd_id}:FG=1"

    def alter_proxy_and_cookie(self, request):
        request.cookies["BAIDUID"] = self.get_cookie()
        if request.meta.get("proxy_enabled"):
            current_proxy = request.meta.get("proxy")
            if current_proxy:
                self.delete_proxy(current_proxy)
            new_proxy = self.get_proxy()
            if new_proxy:
                request.meta["proxy"] = new_proxy
        return request

    def process_request(self, request, spider):
        request.headers["Connection"] = "close"
        request.meta["dont_redirect"] = True
        request.meta["download_timeout"] = 15
        request.cookies["BAIDUID"] = self.get_cookie()
        if request.meta.get("proxy_enabled"):
            request.meta["proxy"] = self.get_proxy()

    def process_response(self, request, response, spider):
        if request.meta.get("dont_retry", False):
            return response
        if response.status in self.retry_http_codes:
            request = self.alter_proxy_and_cookie(request)
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        return response

    def process_exception(self, request, exception, spider):
        if isinstance(exception, self.EXCEPTIONS_TO_RETRY) and not request.meta.get(
            "dont_retry", False
        ):
            request = self.alter_proxy_and_cookie(request)
            return self._retry(request, exception, spider)

    def _retry(self, request, reason, spider):
        max_retry_times = request.meta.get("max_retry_times", self.max_retry_times)
        priority_adjust = request.meta.get("priority_adjust", self.priority_adjust)
        return get_retry_request(
            request,
            reason=reason,
            spider=spider,
            max_retry_times=max_retry_times,
            priority_adjust=priority_adjust,
        )


def get_retry_request(
    request: Request,
    *,
    spider: Spider,
    reason: Union[str, Exception] = "unspecified",
    max_retry_times: Optional[int] = None,
    priority_adjust: Optional[int] = None,
    stats_base_key: str = "retry",
):
    """
    Copied from scrapy source code and made minor changes.
    """
    settings = spider.crawler.settings
    stats = spider.crawler.stats
    retry_times = request.meta.get("retry_times", 0) + 1
    if max_retry_times is None:
        max_retry_times = request.meta.get("max_retry_times")
        if max_retry_times is None:
            max_retry_times = settings.getint("RETRY_TIMES")
    if retry_times <= max_retry_times:
        new_request: Request = request.copy()
        new_request.meta["retry_times"] = retry_times
        new_request.dont_filter = True
        if priority_adjust is None:
            priority_adjust = settings.getint("RETRY_PRIORITY_ADJUST")
        new_request.priority = request.priority + priority_adjust

        if callable(reason):
            reason = reason()
        if isinstance(reason, Exception):
            reason = global_object_name(reason.__class__)

        stats.inc_value(f"{stats_base_key}/count")
        stats.inc_value(f"{stats_base_key}/reason_count/{reason}")
        return new_request
    else:
        stats.inc_value(f"{stats_base_key}/max_reached")
        return f"Gave up retrying {request} (failed {retry_times} times)"
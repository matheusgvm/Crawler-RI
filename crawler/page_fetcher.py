from typing import Optional

from bs4 import BeautifulSoup
from threading import Thread
import requests
from urllib.parse import urlparse, urljoin, ParseResult


class PageFetcher(Thread):
    def __init__(self, obj_scheduler):
        super().__init__()
        self.obj_scheduler = obj_scheduler

    def request_url(self, obj_url: ParseResult) -> Optional[bytes] or None:
        """
        :param obj_url: Instância da classe ParseResult com a URL a ser requisitada.
        :return: Conteúdo em binário da URL passada como parâmetro, ou None se o conteúdo não for HTML
        """

        response = requests.get(obj_url.geturl(), headers={'User-Agent': self.obj_scheduler.usr_agent})

        if 'text/html' not in response.headers.get('Content-Type'):
            return None

        return response.content

    def discover_links(self, obj_url: ParseResult, depth: int, bin_str_content: bytes):
        """
        Retorna os links do conteúdo bin_str_content da página já requisitada obj_url
        """
        soup = BeautifulSoup(bin_str_content, features="lxml")
        for link in soup.select("a"):
            obj_new_url = urlparse(urljoin(obj_url.geturl(), link.get('href')))
            
            if obj_new_url.netloc == obj_url.netloc:
                new_depth = depth + 1
            else:
                new_depth = 0

            yield obj_new_url, new_depth

    def crawl_new_url(self):
        """
        Coleta uma nova URL, obtendo-a do escalonador
        """
        

    def run(self):
        """
        Executa coleta enquanto houver páginas a serem coletadas
        """
        pass

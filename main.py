import typing
import httpx
import asyncio, aiofiles
import pandas as pd

from pathlib import Path

from pandas import DataFrame
from bs4 import BeautifulSoup


class Parser:
    
    @staticmethod
    def get_data_from_gs(gs_url: str) -> DataFrame:
        return pd.read_excel(gs_url.replace('edit?usp=sharing', 'export'))
    
    def perform_data(self, urls_list: typing.Union[typing.List[str], DataFrame, None], gs_url: typing.Optional[str], local: typing.Union[str, Path, None] = None) -> None:
        if local:
            self._data = pd.read_csv(local)
        
        elif gs_url:
            self._data = self.get_data_from_gs(gs_url)
        
        elif isinstance(urls_list, list):
            self._data = DataFrame(urls_list, columns=['url'])
        
        elif isinstance(urls_list, DataFrame):
            self._data = urls_list
        
        print(self._data)

    def perform_client(self):
        self._client = httpx.AsyncClient(proxies='socks5://testvicky2:2a31eb@193.23.50.202:10486')
    
    async def close_client(self):
        if self._client:
            await self._client.aclose()
    
    def __init__(self, *,
                 urls: typing.Union[typing.List[str], DataFrame, None] = None,
                 google_spreadsheet_url: typing.Optional[str] = None,
                 local_csv_url: typing.Union[str, Path, None] = None,
                 loop: asyncio.AbstractEventLoop,
                 save_path: str = './results/',
                 limit: typing.Optional[int] = None):
        self.save_path = save_path
        self._data: typing.Optional[DataFrame] = None
        self._client: typing.Optional[httpx.AsyncClient] = None
        self._tasks: typing.List[asyncio.Task] = []
        self._loop: asyncio.AbstractEventLoop = loop
        self.limit = limit
        self._bad_urls = []
        # self._in_run = []
        
        self.perform_data(urls, google_spreadsheet_url, local_csv_url)
            
    
    async def send_requests(self, url: str):
        try:
            await self.retrieve_data(url)
        except Exception:
            self._bad_urls.append(url)
    
    async def gatherting(self, data):
        await asyncio.gather(*[self._loop.create_task(self.send_requests(url)) for url in data])
    
    async def parse_urls(self):
        
        if isinstance(self._data, type(None)):
            raise SystemExit("You didn't pass any data to parser. Exiting a program")
        try:
            self.perform_client()
            
            limit = len(self._data)
            if self.limit:
                limit = self.limit
            
            await self.gatherting(self._data.iloc[:limit]['url'])
            while len(self._bad_urls):
                bu_copy = self._bad_urls.copy()
                self._bad_urls = []
                await self.gatherting(bu_copy)
                
        except Exception as exc:
            print(exc)
        finally:
            await self.close_client()
        
    async def get_full_name(self, content):
        def wrapper():
            return BeautifulSoup(content, 'html.parser').h1.string
        return await self._loop.run_in_executor(None, wrapper)
    
    async def write_data(self, content: bytes, full_name, url:str):
        path = Path(self.save_path)
        if not path.exists():
            path.mkdir(parents=True)
        try:
            async with aiofiles.open(path / Path(f'{url.split("/")[3]}.html'), 'w') as file:
                await file.write(
                    f"""{url}
{full_name}
{content.decode('utf-8')}
                    """
                )
        except Exception as exc:
            print(exc)
            
    async def retrieve_data(self, url: str):
        if self._client:
            response = await self._client.get(url)
            if response.status_code == 404:
                return
            content = response.content
            full_name = await self.get_full_name(content)
            await self.write_data(content, full_name, url)
    
if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    parser = Parser(local_csv_url=Path('./habr.csv'), loop=loop, limit=50)
    
    loop.run_until_complete(parser.parse_urls())
    
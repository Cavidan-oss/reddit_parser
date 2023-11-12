import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from urllib.parse import urljoin, urlencode
import json

#MAX_CONCURRENT_TABS = 5
PERIOD_TO_GET = 60 * 60 * 24 
BASE_URL = "https://old.reddit.com"
#SUBREDDIT_PATH = '/r/GRE/new/'


class RedditScraper:
    def __init__(self, max_current_tabs, *args, **kwargs):
        self.results = []
        self.max_current_tabs = max_current_tabs
        self.args= args
        self.kwargs = kwargs

        self.semaphore = asyncio.BoundedSemaphore(self.max_current_tabs * 2)

    async def check_date_in_period(self, post_create_date, last_parsed_time, period):
        post_create_date_datetime = datetime.utcfromtimestamp(post_create_date // 1000)
        time_difference = (last_parsed_time - post_create_date_datetime).total_seconds()
        return time_difference <= period
    

    def save_as_json(self, obj, filename, mode='w', indent=4):
        with open(filename, mode) as file:
            json.dump(obj, file, indent=indent)


    async def get_posts(self, page, subreddit_path, attributes_to_extract=['data-fullname', 'data-timestamp', 'data-permalink', 'data-promoted']):
        continue_parsing = True
        subpages = ''

        while continue_parsing:
            print(f"Parsing {BASE_URL + subreddit_path + subpages}")
            await page.goto(BASE_URL + subreddit_path + subpages)

            selected_elements = await page.evaluate(
                '''
                (attributes_to_extract) => Array.from(document.querySelectorAll('div[data-fullname]')).map(element => {
                    const obj = {};
                    attributes_to_extract.forEach(attr => {
                        obj[attr] = element.getAttribute(attr) || null;
                    });
                    return obj;
                })
                ''',
                attributes_to_extract,
            )

            for row in selected_elements:
                is_included_in_period = await self.check_date_in_period(
                    int(row['data-timestamp']), datetime.now(), PERIOD_TO_GET
                )

                if row.get('data-promoted') == 'true':
                    continue

                if not is_included_in_period:
                    continue_parsing = False
                    return

                row.pop('data-promoted')
                self.results.append(row)

            params = {"count": 25, "after": selected_elements[-1].get('data-fullname')}
            subpages = urljoin('/', '?' + urlencode(params))

    async def process_comment(self, result, context):
        async with self.semaphore:
            comment_link = BASE_URL + result.get('data-permalink')
            print(f"Parsing comments for - {comment_link}")
            comment_data = await self.parse_comments(comment_link, context, close_tab=True)
            
            for comment in  comment_data:
                print(comment)

            # self.save_as_json(
            #     comment_data, f"test_parsed_data/{result.get('data-fullname')}.json"
            # )

    async def parse_subredit(self, subreddit_path):
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=False)
            context = await browser.new_context(no_viewport=True)
            page = await context.new_page()

            await self.get_posts(page, subreddit_path)

            tasks = [self.process_comment(result, context) for result in self.results]
            await asyncio.gather(*tasks)

    async def parse_comments(self, comment_path, context, close_tab=False):
        async with self.semaphore:
            page = await context.new_page()
            await page.goto(comment_path)

            html_content = await page.content()
            soup = BeautifulSoup(html_content, 'html.parser')

            heading_data = self.get_heading_data(soup)
            # comments = [
            #     comment for comment in self.get_comments_data(soup=soup, parent_post_id=heading_data.get('Id'))
            # ]

            yield heading_data

            get_comments_data_it = await self.get_comments_data(soup=soup, parent_post_id=heading_data.get('Id'))
                                                                
            for comments_data in get_comments_data_it:
                yield heading_data


            if close_tab:
                await page.close()

            # return {"PostData": heading_data, "CommentData": comments}
        


    def get_comments_data(self, soup, parent_post_id=None):
        comment_divs = soup.find_all('div', class_='thing', attrs={'data-type': 'comment'})

        for comment_div in comment_divs:
            parent_data_fullname = None
            data_fullname = comment_div['data-fullname']
            data_author = comment_div['data-author']
            data_author_id = comment_div['data-author-fullname']
            data_permalink = comment_div['data-permalink']

            comment_text = comment_div.find('div', class_='md').get_text(
                strip=True) if comment_div.find('div', class_='md') else None

            timestamp_element = comment_div.find('time', class_='live-timestamp')
            timestamp = int(
                datetime.fromisoformat(timestamp_element['datetime']).timestamp() * 1000) if timestamp_element else None

            parent_div = comment_div.find_parent(
                'div', class_='thing', attrs={'data-type': 'comment'})

            if parent_div:
                parent_data_fullname = parent_div['data-fullname']

            yield ({"CommentId": data_fullname, "AuthorId" : data_author_id, "Author": data_author, 'Permalink': data_permalink, 'Comment': comment_text,
                    'ParentCommentId': parent_data_fullname, "ParentPostId": parent_post_id, "CreatedAt": timestamp})

    def get_heading_data(self, soup):
        data_fullname = soup.select_one('.thing')['data-fullname']
        permalink = soup.select_one('.title a')['href']
        timestamp = soup.select_one('.thing')['data-timestamp']
        data_author = soup.select_one('.thing')['data-author']
        data_author_id = soup.select_one('.thing')['data-author-fullname']

        post_text = soup.select_one('.thing .md').get_text(
            strip=True) if soup.select_one('.thing .md') else None

        return ({"PostId": data_fullname, "AuthorId" : data_author_id, "AuthorName": data_author, 'Permalink': permalink,
                    'Comment': post_text, "CreatedAt": timestamp})


if __name__ == '__main__':
    scraper = RedditScraper(max_current_tabs = 6)
    asyncio.run(scraper.parse_subredit('/r/GRE/new/'))
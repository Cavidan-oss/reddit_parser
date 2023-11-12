from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import json
from urllib.parse import urljoin, urlencode


PERIOD_TO_GET = 60 * 60 * 24 * 7 
BASE_URL = "https://old.reddit.com"
SUBREDDIT_PATH = '/r/GRE/new/'
MAX_CONCURRENT_TABS = 8

async def check_date_in_period(post_create_date, last_parsed_time, period):
    post_create_date_datetime = datetime.utcfromtimestamp(post_create_date // 1000)
    time_difference = (last_parsed_time - post_create_date_datetime).total_seconds()
    return time_difference <= period


def save_as_json(obj, filename, mode='w', indent=4):
    with open(filename, mode) as file:
        json.dump(obj, file, indent=indent)


async def get_posts(page, subreddit_path, attributes_to_extract=['data-fullname', 'data-timestamp', 'data-permalink', 'data-promoted']):
    continue_parsing = True
    results = []
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
            is_included_in_period = await check_date_in_period(int(row['data-timestamp']), datetime.now(), PERIOD_TO_GET)

            if row.get('data-promoted') == 'true':
                continue

            if not is_included_in_period:
                continue_parsing = False
                return results

            row.pop('data-promoted')
            results.append(row)

        params = {"count": 25, "after": selected_elements[-1].get('data-fullname')}
        subpages = urljoin('/', '?' + urlencode(params))

    return results


async def main():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context(no_viewport=True)
        page = await context.new_page()

        results = await get_posts(page, SUBREDDIT_PATH)

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TABS)
        tasks = []

        async def process_comment(result):
            async with semaphore:
                comment_link = BASE_URL + result.get('data-permalink')
                print(f"Parsing comments for - {comment_link}")
                return await parse_comments(comment_link, context, close_tab=True)

        for result in results:
            tasks.append(process_comment(result))

        comment_data_list = await asyncio.gather(*tasks)

        for result, comment_data in zip(results, comment_data_list):
            save_as_json(comment_data, f"test_parsed_data/{result.get('data-fullname')}.json")

        print(len(results))

    

async def get_comments_data(soup, parent_post_id=None):
    comment_divs = soup.find_all('div', class_='thing', attrs={'data-type': 'comment'})

    for comment_div in comment_divs:
        parent_data_fullname = None
        data_fullname = comment_div['data-fullname']
        data_author = comment_div['data-author']
        data_permalink = comment_div['data-permalink']

        comment_text = comment_div.find('div', class_='md').get_text(strip=True) if comment_div.find('div',
                                                                                                   class_='md') else None

        timestamp_element = comment_div.find('time', class_='live-timestamp')
        timestamp = int(
            datetime.fromisoformat(timestamp_element['datetime']).timestamp() * 1000) if timestamp_element else None

        parent_div = comment_div.find_parent('div', class_='thing', attrs={'data-type': 'comment'})

        if parent_div:
            parent_data_fullname = parent_div['data-fullname']

        yield (
            {"Id": data_fullname, "Author": data_author, 'Permalink': data_permalink, 'Comment': comment_text,
             'ParentCommentId': parent_data_fullname, "ParentPostId": parent_post_id, "CreatedAt": timestamp})


async def get_heading_data(soup):
    data_fullname = soup.select_one('.thing')['data-fullname']
    permalink = soup.select_one('.title a')['href']
    timestamp = soup.select_one('.thing')['data-timestamp']
    data_author = soup.select_one('.thing')['data-author']
    post_text = soup.select_one('.thing .md').get_text(strip=True) if soup.select_one('.thing .md') else None

    return ({"Id": data_fullname, "Author": data_author, 'Permalink': permalink, 'Comment': post_text,
             "CreatedAt": timestamp})


async def parse_comments(comment_path, context, close_tab=False):
    page = await context.new_page()
    await page.goto(comment_path)

    html_content = await page.content()
    soup = BeautifulSoup(html_content, 'html.parser')

    heading_data = await get_heading_data(soup)
    comments = [comment async for comment in get_comments_data(soup=soup, parent_post_id=heading_data.get('Id'))]

    if close_tab:
        await page.close()

    return {"PostData": heading_data, "CommentData": comments}


if __name__ == '__main__':
    import asyncio

    asyncio.run(main())

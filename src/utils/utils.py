import requests
from bs4 import BeautifulSoup

# utils.py
def _python_extract_attribute(divs, keyword):
    filtered_divs = [div for div in divs if keyword in div]
    if len(filtered_divs) == 1:
        return filtered_divs[0].split(':')[1].strip()
    return None

def _python_extract_rank(divs, keyword, not_ranked='not ranked', split_term=None):
    filtered_divs = [div for div in divs if keyword.lower() in div.lower()]
    if len(filtered_divs) == 1:
        div_text = filtered_divs[0].lower()
        if not_ranked in div_text:
            return None
        if split_term:
            rank_text = div_text.split(split_term.lower())[1].split('.')[0]
            if 'best rank' in rank_text:
                return rank_text.split('best rank')[1].strip()
            return rank_text.strip()
    return None

def extract_information(divs):
    filtered_divs = [div for div in divs if 'Man,' in div or 'Woman,' in div]
    gender = ['Man' if 'Man,' in div else 'Woman' if 'Woman,' in div else None for div in filtered_divs][0]
    if len(filtered_divs) == 1:
        div = filtered_divs[0]
        if gender:
            parts = div.split(',')[1]
            class_status = parts.split(' (')[1].replace(')',''). strip() if ' (' in parts else None
        else:
            class_status = None
    else:
         class_status = None
    return class_status

def _python_rank_list_urls(date,headers):
    # Rank list URL
    url = f'https://www.ipttc.org/rating/{date}/index.htm'
    response = requests.get(url,headers=headers,timeout=10)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all the tables on the page
    tables = soup.find_all('table')

    links = []
    for table in tables:
        # Find all link elements within the table
        for link in table.find_all('a', href=True):
            if 'wheelchair' not in link['href'] and 'standing' not in link['href']:
                # Add the link URL to the list
                links.append(
                    f"https://www.ipttc.org/rating/{date}/" 
                    f"{link['href']}"
                    )
    return links


def _python_parse_rank_list(link,date,headers):
    link_short_1 = link.split(f'https://www.ipttc.org/rating/{date}/')[1]
    event_name = link_short_1.split('/')[0]

    rank_response = requests.get(link,headers=headers,timeout=10)
    rank_soup = BeautifulSoup(rank_response.content, "html.parser")

    table = rank_soup.find("table", {"class": "lists table table-striped table-condensed table-bordered"})

    # Extract table rows
    rows = table.find_all("tr")

    # Extract data and populate DataFrame
    data = []
    for row in rows[1:]:  # Skip header row
        cols = row.find_all("td")

        if len(cols) > 1:  # Ensure row has data
            rank = cols[0].get_text(strip=True)
            player_id = cols[1].get_text(strip=True)
            name = cols[2].get_text(strip=True)
            country = cols[3].get_text(strip=True)
            region = cols[4].get_text(strip=True)
            rating = cols[5].get_text(strip=True)
            prev_rating = cols[6].get_text(strip=True)

            link_short_2 = link_short_1.split('/')[1].split('.htm')[0]
            class_name = link_short_2[1:]
            gender = link_short_2[0]

            ranking_dict = {
                "rank_list_date":date,
                "rank_list_link":link,
                "rank":rank,
                "player_id":player_id,
                "full_name":name,
                "country":country,
                "region":region,
                "event_name":event_name,
                "class_name":class_name,
                "gender":gender,
                "rating":rating,
                "previous_rating":prev_rating,
            }

            data.append(ranking_dict)

    return data
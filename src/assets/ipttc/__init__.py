''' 
API calls to and html webscrape of the IPTTC website
'''

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import date, datetime
import re
import time
from datetime import datetime
import json
from dagster import (asset,
                     AssetIn,
                     multi_asset,
                     AssetOut,
                     Output,
                     get_dagster_logger)
from utils.utils import _python_extract_attribute, _python_extract_rank, extract_information, _python_rank_list_urls, _python_parse_rank_list
from utils.constants import disciplines

headers = {
    'accept': 'application/json',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'referer': f'https://results.ittf.com/',
}

@asset(
    compute_kind='python',
    group_name='ipttc',
    io_manager_key='snowflake_raw',
    key_prefix=['raw', 'ipttc']
)
def _python_raw_competitions() -> pd.DataFrame:
    page_response = requests.get('http://stats.ipttc.org/tournaments',timeout=10)
    page_soup = BeautifulSoup(page_response.content, 'html.parser')
    page_max = int(page_soup.find("li", attrs={"class":'last next'}).find('a')['href'].split('page=')[1])
    
    all_tournaments = []
    for page in range(1, page_max +1):
        print(f"Scraping Page {page}/{page_max}")
        comp_list_response = requests.get(f"http://stats.ipttc.org/en/tournaments?page={page}")
        comp_list_soup = BeautifulSoup(comp_list_response.content, 'html.parser')

        # Extract table and rows
        table = comp_list_soup.find("table", {"id": "tournaments"})
        rows = table.find_all("tr")

        # Extract data and populate DataFrame
        for row in rows[1:]:  # Skip header row
            cols = row.find_all("td")

            if len(cols) > 1:  # Ensure row has data
                comp_name = cols[1].get_text(strip=True)
                comp_url = cols[1].find('a',href=True)['href']
                comp_id = comp_url.split('tournaments/')[1].strip()
                city = cols[2].get_text(strip=True)
                country = cols[3].get_text(strip=True)
                start_date = cols[4].get_text(strip=True)
                
                all_tournaments.append([int(comp_id),comp_name,comp_url,city,country,start_date])

    return pd.DataFrame(all_tournaments, columns=["competition_id", "competition_name","competition_url","city","country","start_date"])

@asset(
    ins={'_python_raw_competitions': AssetIn(key_prefix=['raw', 'ipttc'],
                                    metadata={'columns': ['competition_id']})},
    compute_kind='python',
    group_name='ipttc',
    io_manager_key='snowflake_raw',
    key_prefix=['raw', 'ipttc']
)
def _python_raw_event_info(_python_raw_competitions: pd.DataFrame) -> pd.DataFrame:
    all_event_info = []
    competition_ids = _python_raw_competitions['competition_id'].values
    for i, comp_id in enumerate(competition_ids):
        print(f"Scraping Comp ID {comp_id} ({i+1}/{len(competition_ids)})")
        
        time.sleep(np.random.randint(1,3))
        comp_info_url = f"https://results.ittf.com/ittf-web-results/html/TTE{comp_id}/champ.json"

        try:
            comp_info_response = requests.get(comp_info_url,headers=headers,timeout=10).json()
            comp_info_response['competitionId'] = comp_id

            all_event_info.append(comp_info_response)

        except:
            continue
        # Break out of loop as competitions before 5345 do not have extended results accessible through API
        if comp_id == 5345:
            break
        else:
            continue

    return pd.DataFrame(all_event_info)

@asset(
    ins={'_python_raw_event_info': AssetIn(key_prefix=['raw', 'ipttc'],
                                    metadata={'columns': ['competitionid','dates']})},
    compute_kind='python',
    group_name='ipttc',
    io_manager_key='snowflake_raw',
    key_prefix=['raw', 'ipttc']
)
def _python_raw_results(_python_raw_event_info: pd.DataFrame) -> pd.DataFrame:
    _python_raw_event_info['dates'] = _python_raw_event_info['dates'].apply(json.loads)

    # Initialize an empty list to store all (comp_ids, comp_date) pairs
    all_combinations = []

    # Iterate over each competition ID and its corresponding list of dates
    for comp_ids, dates in zip(_python_raw_event_info['competitionid'].values, _python_raw_event_info['dates'].values):
        for nested_date in dates:
            try:
                # Extract the 'raw' date from each nested_date dictionary
                comp_date = nested_date['raw']
                # Append the combination of comp_id and comp_date to the list
                all_combinations.append((comp_ids, comp_date))
            except KeyError:
                # Continue to the next nested_date if 'raw' key is not found
                continue

    all_results = []
    get_dagster_logger().info(f"Number of Comp/Date pairs: {len(all_combinations)}")
    for i, (comp_id, date) in enumerate(all_combinations):
        print(
            f"Scraping Comp ID: {comp_id} on {date}"
            f" ------------> "
            f"{i+1}/{len(all_combinations)}"
            )

        result_response = requests.get(
            f'https://results.ittf.com/ittf-web-results/html/TTE{comp_id}/match/d{date}.json',
            headers=headers,
            timeout=10
        ).json()

        for result in result_response:
            if len(list(result.keys())) > 13:
                get_dagster_logger().info(
                                        f"{comp_id} on {comp_date}\n"
                                        f"Response Keys: {list(result.keys())}"
                                        )
            # Get the values with defaults if the keys are not present
            result['Duration'] = result.get('Duration', None)
            result['Periods'] = result.get('Periods', None)
            result['SubMatches'] = result.get('SubMatches', None)
            
            result['MatchDate'] = date
            result['CompetitionId'] = comp_id
            all_results.append(result)

            time.sleep(1)

    return pd.DataFrame(all_results)

@asset(
    compute_kind='python',
    group_name='ipttc'
)
def _python_profile_page_count() -> int:
    raw_profile_url = "http://stats.ipttc.org/en/profiles?utf8=%E2%9C%93&player_id=&name=&gender=&country_id=&classification=&classification_status=&button="
    raw_profile_response = requests.get(raw_profile_url,timeout=10)
    raw_profile_soup = BeautifulSoup(raw_profile_response.content, 'html.parser')

    total_pages = int(raw_profile_soup.find('a',string='Last')['href'].split('page=')[-1].split("&")[0])
    
    return total_pages

@asset(
    ins={'_python_profile_page_count': AssetIn()},
    compute_kind='python',
    group_name='ipttc',
    io_manager_key='snowflake_raw',
    key_prefix=['raw', 'ipttc']
)
def _python_raw_profile_short(_python_profile_page_count: int) -> pd.DataFrame:
    all_profile_short_dfs = []
    for page in range(1,_python_profile_page_count+1):
        print(f"Scraping Page {page}/{_python_profile_page_count}")
        page_url = f"http://stats.ipttc.org/en/profiles?button=&classification=&classification_status=&country_id=&gender=&name=&page={page}&player_id="
        page_response = requests.get(page_url,timeout=10)
        page_soup = BeautifulSoup(page_response.content)
            
        time.sleep(np.random.randint(1,5))

        table = page_soup.find("table", {"class": "table table-striped table-condensed"})
        rows = table.find_all("tr")

        for row in rows[1:]:  # Skip header row
            cols = row.find_all("td")
            if len(cols) > 1:  # Ensure row has data
                    row = {
                         "name": cols[0].get_text(strip=True),
                         "player_url": cols[0].find('a').get('href'),
                         "player_id": cols[0].find('a').get('href').split('en/profiles/')[1],
                         "gender": cols[1].get_text(strip=True),
                         "class_name": cols[2].get_text(strip=True),
                         "noc_short": cols[3].get_text(strip=True),
                    }

                    all_profile_short_dfs.append(row)

    return pd.DataFrame(all_profile_short_dfs)

@asset(
    ins={'_python_raw_profile_short': AssetIn(key_prefix=['raw', 'ipttc'],
                                    metadata={'columns': ['player_url','player_id']})},
    compute_kind='python',
    group_name='ipttc',
    io_manager_key='snowflake_raw',
    key_prefix=['raw', 'ipttc']
)
def _python_raw_profile_extended(_python_raw_profile_short: pd.DataFrame) -> pd.DataFrame:
    player_urls = _python_raw_profile_short['player_url'].values # Converted to list for testing purposes
    player_ids = _python_raw_profile_short['player_id'].values # Converted to list for testing purposes

    all_player_profiles_extended = []

    # Use enumerate to get the index (i), and zip to iterate over both lists simultaneously
    for i, (player_url_short, player_id) in enumerate(zip(player_urls, player_ids)):
        print(f"Scraping Player ID {player_id} ({i+1}/{len(player_ids)})")

        player_url = f"http://stats.ipttc.org/{player_url_short}"
        player_response = requests.get(player_url,timeout=10)
        page_soup = BeautifulSoup(player_response.content,'html.parser')
        
        # Locate the div container you're interested in and get the divs
        container = page_soup.find('div', class_='col-lg-6 col-md-6 col-sm-12 col-xs-12')
        divs = [i.text for i in container.find_all('div', recursive=False)]

        # gender,class_name,class_status = extract_information(divs)
        class_status = extract_information(divs)

        age_div = next((div for div in divs if 'years old' in div), None)
        age = age_div.split(' years old')[0].strip() if age_div else None

        current_rank = _python_extract_rank(divs, 'ranked', 'not ranked', 'Ranked')
        best_rank = _python_extract_rank(divs, 'best rank', split_term = 'best rank')
        first_tournament = _python_extract_attribute(divs, 'First tournament:')
        last_tournament = _python_extract_attribute(divs, 'Last tournament:')
        height = _python_extract_attribute(divs, 'Height:')
        impairment = _python_extract_attribute(divs, 'Impairment:')
        profession = _python_extract_attribute(divs, 'Profession:')
        club = _python_extract_attribute(divs, 'Club:')
        coach = _python_extract_attribute(divs, 'Personal:')
        residence = _python_extract_attribute(divs, 'Residence:')
        
        # Store as Dict
        player_profile_dict = {
            'player_id':player_id,
            'class_status':class_status,
            'age':age,
            'residence':residence,
            'club':club,
            'coach':coach,
            'impairment':impairment,
            'profession':profession,
            'current_rank':current_rank,
            'best_rank':best_rank,
            'first_tournament':first_tournament,
            'last_tournament':last_tournament
        }
        all_player_profiles_extended.append(player_profile_dict)
    
    return pd.DataFrame(all_player_profiles_extended)

@asset(
    compute_kind='python',
    group_name='ipttc',
    io_manager_key='snowflake_raw',
    key_prefix=['raw', 'ipttc']
)
def _python_raw_ranking_info() -> pd.DataFrame:
    # Parse the HTML content
    rank_info_response = requests.get('https://www.ipttc.org/rating/',headers=headers,timeout=10)
    rank_info_soup = BeautifulSoup(rank_info_response.content, "html.parser")

    table = rank_info_soup.find("table", {"id": "ranking-lists"})

    # Extract table rows
    rows = table.find_all("tr")

    # Extract data and populate DataFrame
    data = []
    for row in rows[1:]:  # Skip header row
        cols = row.find_all("td")
        if len(cols) > 1:  # Ensure row has data
            date = cols[0].get_text(strip=True)
            note = cols[1].get_text(strip=True, separator=" ")
            note = re.sub(r'\s+', ' ', note) # remove double spaces
            data.append([date, note])
    
    return pd.DataFrame(data, columns=["ranking_list_date", "additional_notes"])


@asset(
    ins={'_python_raw_ranking_info': AssetIn(key_prefix=['raw', 'ipttc'],
                                    metadata={'columns': ['ranking_list_date']})},
    compute_kind='python',
    group_name='ipttc',
    io_manager_key='snowflake_raw',
    key_prefix=['raw', 'ipttc']
)
def _python_raw_rankings(_python_raw_ranking_info: pd.DataFrame):
    all_rank_dicts = []
    rank_dates = _python_raw_ranking_info['ranking_list_date'].values[0:1]
    for i, rank_date in enumerate(rank_dates):
            print(f"Scraping date: {rank_date} ({i+1}/{len(rank_dates)})")
            links = _python_rank_list_urls(rank_date, headers=headers)
            print("All links found...")
            for link in links[0:2]:
                  time.sleep(60)
                  print(f"--> Link {links.index(link)+1}/{len(links)} -> {link}")

                  for rank_dict in _python_parse_rank_list(link,rank_date, headers=headers):
                        all_rank_dicts.append(rank_dict)

            time.sleep(np.random.randint(60)) 

    return pd.DataFrame(all_rank_dicts)
import random
import time
import datetime
import json

import sys

sys.path.append('/home/lucas/pipelines/helpers/')

from helpers import Helpers

from airflow.hooks.base import BaseHook
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

class HltvHook(BaseHook):
    def __init__(self, execution_date):
        self.execution_date = datetime.datetime.fromisoformat(execution_date)

    def wait(self, seconds=None):
        if not seconds:
            seconds = random.randrange(2,8)
        time.sleep(seconds)

    def create_driver(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument("--remote-debugging-port=9222")
        chrome_options.add_argument('--disable-dev-shm-usage')   

        self.driver = webdriver.Chrome(options=chrome_options, service=Service(ChromeDriverManager().install()))

    def get_match_links(self):
        count_offset = 0

        self.match_links = []
        found_date = False
        dont_have_games = False

        while (not(found_date)):
            url = f'https://www.hltv.org/results?offset={count_offset}'
            self.driver.get(url)

            search_date = (self.execution_date - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
            self.wait(3)
            results_per_day = self.driver.find_elements(By.XPATH, '//div[@class="results-all"]//div[@class="results-sublist"]')

            for result in results_per_day:
                date_string = result.find_element(By.XPATH, 'div[@class="standard-headline"]')\
                .get_attribute('innerHTML')\
                .split(' ')

                day = date_string[3].replace('th', '').replace('st', '').replace('nd', '').replace('rd', '')
                month = int(Helpers().treat_month_names(date_string[2]))
                year = date_string[4]

                date_div = datetime.datetime(int(year), int(month), int(day)).strftime('%Y-%m-%d')

                if(search_date == date_div):
                    found_date = True
                    match_divs = result.find_elements(By.XPATH, 'div[@class="result-con"]')

                    for match_div in match_divs:
                        match_link = match_div.find_element(By.XPATH, 'a').get_attribute('href')
                        self.match_links.append(match_link)

                    break
                if(search_date > date_div):
                    dont_have_games = True
            
            if(dont_have_games):
                break

            if(not(found_date)):
                count_offset += 100
            

    def get_match_teams(self, teams):
        match_teams_list = []
        
        for team in teams:
            team_name = team.find_element(By.XPATH, 'div//a//div').get_attribute('innerHTML')
            score = team.find_element(By.XPATH, 'div/div').get_attribute('innerHTML')
            team_link = team.find_element(By.XPATH, 'div/a').get_attribute('href')
                
            match_team = {}
            match_team['team_id'] = team_link.split('/')[4]
            match_team['name'] = team_name
            match_team['score'] = score
            match_team['link'] = team_link
            
            match_teams_list.append(match_team)
        
        return match_teams_list

    def get_team_result(self, result, side, team_sides):
        team_selector = 'div' if side == 'left' else 'span'
        
        team_name = result.find_element(
                By.XPATH, 
                f'div[@class="results played"]/{team_selector}[contains(@class, "results-{side}")]//div[@class="results-teamname-container text-ellipsis"]'
                '/div[@class="results-teamname text-ellipsis"]')\
            .get_attribute('innerHTML')
        
        
        team_result = result.find_element(
            By.XPATH, 
            f'div[@class="results played"]/{team_selector}[contains(@class, "results-{side}")]//div[@class="results-team-score"]')\
        .get_attribute('innerHTML')
        
        team_side_half1 = team_sides[1 if side == 'left' else 3].get_attribute('class')
        team_result_half1 = team_sides[1 if side == 'left' else 3].get_attribute('innerHTML')
        
        team_side_half2 = team_sides[5 if side == 'left' else 7].get_attribute('class')
        team_result_half2 = team_sides[5 if side == 'left' else 7].get_attribute('innerHTML')
        
        map_result = {}
        map_result['team_name'] = team_name
        map_result['result'] = team_result
        map_result[team_side_half1] = team_result_half1
        map_result[team_side_half2] = team_result_half2
        
        return map_result

    def get_results_by_map(self, result_div, map):
        map_results = []
        
        was_played = result_div.find_element(By.XPATH, 'div').get_attribute('class') == 'played'
        map['was_played'] = was_played
        
        if(was_played):
            team_sides = result_div.find_elements(
                By.XPATH, 
                'div[@class="results played"]/div[contains(@class, "results-center")]'
                '//div[@class="results-center-half-score"]/span')

            team_left = self.get_team_result(result_div, 'left', team_sides)
            team_right = self.get_team_result(result_div, 'right', team_sides)
            
            map_results.append(team_left)
            map_results.append(team_right)
        
        return map_results

    def get_results(self, maps, results_div):
        for map in maps:
            for result_div in results_div:
                map_name = result_div.find_element(By.XPATH, 'div/div/div').get_attribute('innerHTML')
                
                if(map_name == map['map_name']):
                    map['results'] = self.get_results_by_map(result_div, map)
                    break

    def get_maps(self, maps, match):
        game_type_div = maps.find_element(By.XPATH, 'div/div/div')
        
        game_type = game_type_div.get_attribute('innerHTML').split('*')[0].replace('\n', '').split('(')
        
        game_total_maps = game_type[0]
        game_lan_online = game_type[1].replace(')', '')

        match['type'] = game_lan_online
        match['best_of_type'] = game_total_maps
        
        maps_in_one_div = False

        maps_pool_divs = self.driver.find_elements(
            By.XPATH, 
            '//div[@class="standard-box veto-box"]/div[@class="padding"]/div')
        
        if(len(maps_pool_divs) == 0):
            maps_in_one_div = True
            maps_text = self.driver.find_element(
                By.XPATH, 
                '//div[@class="standard-box veto-box"]/div').get_attribute('innerHTML')

            maps_list = maps_text.split('\n')
            maps_pool_divs = maps_list[5:]

        maps = {}
        maps['bans'] = []
        maps['picks'] = []
        
        for map_div in maps_pool_divs:
            map_describe = ''
        
            if(maps_in_one_div):
                map_describe = map_div
            else:
                map_describe = map_div.get_attribute('innerHTML')

            found_team = False
            
            for team in match['teams']:
                if(team['name'] in map_describe):
                    found_team = True
                    map_name = ''
                
                    if(maps_in_one_div & (len(map_describe.split(' ')) > 8)):
                        map_name = map_describe.split(' ')[-6]
                    else: 
                        map_name = map_describe.split(' ')[-1]
                    
                    map = {}
                    map['team_id'] = team['team_id'] if team['name'] in map_describe else None
                    map['choice_number'] = map_describe.split(' ')[0].replace('.', '')
                    map['map_name'] = map_name
                    
                    if('removed' in map_describe):
                        maps['bans'].append(map)
                    else:
                        maps['picks'].append(map)
                
                    break
            
            if(not(found_team)):
                choice_number = map_describe.split(' ')[0].replace('.', '')
                
                map = {}
                map['team_id'] = None
                map['choice_number'] = choice_number
                map['map_name'] = map_describe.split(' ')[1] if choice_number == '7' else map_describe.split(' ')[-1]
                
                if('removed' in map_describe):
                    maps['bans'].append(map)
                else:
                    maps['picks'].append(map)
        
        return maps

    def remove_unplayed_maps(self, match):
        maps = []
        
        for map in match['maps']['picks']:
            if(not('was_played' in map)):
                continue
            
            if(map['was_played'] == False):
                continue
                
            maps.append(map)
            
        match['maps']['picks'] = maps

    def get_stats_team(self, stat, team_id):
        players = []
        team_player_rows = stat.find_elements(By.XPATH, 'tbody/tr')
        
        for index, tpr in enumerate(team_player_rows):
            if(index == 0):
                continue
            
            player_link = tpr.find_element(By.XPATH, 
                'td[@class="players"]//a')\
                .get_attribute('href')
            
            player = {}
            
            player['player_id'] = player_link.split('/')[-2]
            player['team_id'] = team_id
            player['nick'] = player_link.split('/')[-1]
            player['kd'] = tpr.find_element(By.XPATH, 'td[@class="kd text-center"]')\
                        .get_attribute('innerHTML')
            player['diff'] = tpr.find_element(By.XPATH, 'td[contains(@class, "plus-minus")]/span')\
                            .get_attribute('innerHTML')
            player['adr'] = tpr.find_element(By.XPATH, 'td[contains(@class, "adr")]').get_attribute('innerHTML')
            player['kast'] = tpr.find_element(By.XPATH, 'td[contains(@class, "kast")]').get_attribute('innerHTML')
            player['rating'] = tpr.find_element(By.XPATH, 'td[contains(@class, "rating")]').get_attribute('innerHTML')
        
            players.append(player)
            
        return players

    def get_player_stats(self, players, map_name):
        stats = players.find_elements(By.XPATH, 'table')
        
        team1_id = stats[0].find_element(By.XPATH, 
        'tbody/tr[@class="header-row"]/td[@class="players"]//a[@class="teamName team"]')\
        .get_attribute('href').split('/')[-2]
    
        team2_id = stats[3].find_element(By.XPATH, 
        'tbody/tr[@class="header-row"]/td[@class="players"]//a[@class="teamName team"]')\
        .get_attribute('href').split('/')[-2]

        stats_map = {}
        stats_map['map'] = map_name
        stats_map['team1'] = {}
        stats_map['team2'] = {}
        
        stats_map['team1']['total'] = self.get_stats_team(stats[0], team1_id)
        stats_map['team1']['tr'] = self.get_stats_team(stats[1], team1_id)
        stats_map['team1']['ct'] = self.get_stats_team(stats[2], team1_id)

        stats_map['team2']['total'] = self.get_stats_team(stats[3], team2_id)
        stats_map['team2']['tr'] = self.get_stats_team(stats[4], team2_id)
        stats_map['team2']['ct'] = self.get_stats_team(stats[5], team2_id)
        
        return stats_map

    def get_all_stats(self, maps_stats_divs):
        stats = []
        
        for index, msd in enumerate(maps_stats_divs):
            map_name = msd.find_element(
                By.XPATH, 
                'div[contains(@class, "stats-menu-link")]'
                '/div[contains(@class, "dynamic-map-name-full")]')\
                .get_attribute('innerHTML')
            
            players_div = self.driver.find_elements(By.XPATH, '//div[@class="matchstats"]//div[@class="stats-content"]')
            stats.append(self.get_player_stats(players_div[index], map_name))
        
        return stats

    def get_matchs(self):
        matchs = []
        count = 0

        for match_link in self.match_links:
            self.driver.get(match_link)
            count+= 1
            
            match = {}
            
            match['match_id'] = match_link.split('/')[4]
            maps_div = self.driver.find_element(By.XPATH, '//div[contains(@class, "maps")]')
            match_describe = maps_div.find_element(By.XPATH, 'div/div/div')
            
            is_forfeit = 'forfeit' in match_describe.get_attribute('innerHTML')
            
            if(not(is_forfeit)):
                teams_div = self.driver.find_elements(By.XPATH, '//div[@class="team"]')
                match['teams'] = self.get_match_teams(teams_div)

                maps_stats_divs = self.driver.find_elements(By.XPATH, 
                    '//div[@class="matchstats"]'
                    '//div[@class="box-headline flexbox nowrap header"]'
                    '/div[@class="flexbox nowrap"]/div')

                match['stats'] = self.get_all_stats(maps_stats_divs)

                match['maps'] = self.get_maps(maps_div, match)

                results_div = maps_div.find_elements(By.XPATH, 'div/div[@class="flexbox-column"]/div[@class="mapholder"]')
                self.get_results(match['maps']['picks'], results_div)

                self.remove_unplayed_maps(match)

                matchs.append(match)
            
            #if(count == 2):
                #break

        return matchs

    def run(self):
        self.create_driver()

        self.wait(3)

        self.get_match_links()

        return self.get_matchs()

if __name__ == "__main__":
    result = HltvHook('2022-02-25').run()
    print(json.dumps(result, indent=4, sort_keys=True))
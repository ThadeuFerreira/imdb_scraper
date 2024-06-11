import requests
import asyncio
import aiohttp
from aiohttp import ClientSession
from bs4 import BeautifulSoup
import pandas as pd
from typing import List
#Load variables from .env file
from dotenv import load_dotenv
import os

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

load_dotenv()

START_ID = os.getenv("START_ID")
if START_ID is None:
    START_ID = 1
END_ID = os.getenv("END_ID")
if END_ID is None:
    END_ID = 100
BATCH_SIZE = os.getenv("BATCH_SIZE")
if BATCH_SIZE is None:
    BATCH_SIZE = 10

# Define a semaphore with a specific limit for concurrent requests
SEM_LIMIT = os.getenv("SEM_LIMIT")  # Adjust this value based on your requirements and server limits
if SEM_LIMIT is None:
    SEM_LIMIT = 10

async def fetch_movie_info(session, movie_url, semaphore):
    async with semaphore:
        try:
            async with session.get(movie_url, headers=headers) as response:
              # Check if the response status code is 200 (OK)
                if response.status == 200:
                  response.raise_for_status()

                  html = await response.text()
                  soup = BeautifulSoup(html, 'html.parser')

                  # Title
                  # Default Title
                  title_tag = soup.find('h1')
                  title = title_tag.text.strip() if title_tag else 'N/A'
                  print(title)

                  # Rating
                  rating_tag = soup.find('span', class_='sc-bde20123-1 cMEQkK')
                  rating = rating_tag.text.strip() if rating_tag else 'N/A'

                  # Director
                  director_tag = soup.find('li', {'data-testid': 'title-pc-principal-credit'})
                  director = director_tag.a.text.strip() if director_tag and director_tag.a else 'N/A'


                  # Plot
                  plot_tag = soup.find('span', {'data-testid': 'plot-xl'})
                  plot = plot_tag.text.strip() if plot_tag else 'N/A'

                  # Genres
                  genres_tag = soup.find('div', {'data-testid': 'genres'})
                  genres = [genre.text for genre in genres_tag.find_all('span', class_='ipc-chip__text')] if genres_tag else []

                  ## Writers
                  writers = []
                  try:
                      writers_tag = soup.find_all('li', {'data-testid': 'title-pc-principal-credit'})[1]
                      writers = [writer.text.strip() for writer in writers_tag.find_all('a')]
                  except (IndexError, AttributeError):
                      writers = []

                  # Stars
                  stars = []
                  try:
                      stars_tag = soup.find_all('li', {'data-testid': 'title-pc-principal-credit'})[2]
                      stars = [star.text.strip() for star in stars_tag.find_all('a')]
                  except (IndexError, AttributeError):
                      stars = []

                    # Year, Age Classification, Duration
                  details_list = soup.find('ul', class_='ipc-inline-list ipc-inline-list--show-dividers sc-d8941411-2 cdJsTz baseAlt')
                  if details_list:
                      details_items = details_list.find_all('li')
                      year = details_items[0].text.strip() if len(details_items) > 0 else 'N/A'
                      age_classification = details_items[1].text.strip() if len(details_items) > 1 else 'N/A'
                      duration = details_items[2].text.strip() if len(details_items) > 2 else 'N/A'
                  else:
                      year = 'N/A'
                      age_classification = 'N/A'
                      duration = 'N/A'

                  return {
                      'URL': movie_url,
                      'Title': title,
                      'Rating': rating,
                      'Director': director,
                      'Plot': plot,
                      'Genres': genres,
                      'Writers': writers,
                      'Stars': stars,
                      'Year': year,
                      'Age Classification': age_classification,
                      'Duration': duration
                  }
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            return None


async def fetch_all_movies(movies, semaphore):
    async with ClientSession() as session:
        tasks = [fetch_movie_info(session, movie_url, semaphore) for movie_url in movies]
        return await asyncio.gather(*tasks)

def get_actor_info(actor_url):
    try:
        response = requests.get(actor_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        name = soup.find('span', class_='itemprop').text.strip()
        birth_date = soup.find('time')['datetime'] if soup.find('time') else 'N/A'
        bio = soup.find('div', class_='inline').text.strip() if soup.find('div', class_='inline') else 'N/A'

        return {
            'Name': name,
            'Birth Date': birth_date,
            'Biography': bio
        }
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None




def generate_movies_id(start, batch_size) -> List[str]:
    #'tt0111161'
    s = 'tt'
    l = []
    for i in range(start,start+batch_size):
        l.append(s + str(i).zfill(7))
    return l




movies = [
    'https://www.imdb.com/title/tt0111161/',  # The Shawshank Redemption
    'https://www.imdb.com/title/tt0068646/'   # The Godfather
]

actors = [
    'https://www.imdb.com/name/nm0000209/',  # Al Pacino
    'https://www.imdb.com/name/nm0000151/'   # Morgan Freeman
]


def generate_movies_id(start, batch_size) -> List[str]:
    #'tt0111161'
    s = 'tt'
    l = []
    for i in range(start,start+batch_size):
        l.append(s + str(i).zfill(7))
    return l



async def get_data():
    semaphore = asyncio.Semaphore(SEM_LIMIT)
    for i in range(START_ID, END_ID, BATCH_SIZE):
        movies = []
        movie_ids = list(generate_movies_id(i, BATCH_SIZE))

        for id in movie_ids:
            m = f'https://www.imdb.com/title/{id}/'
            movies.append(m)

        
        movie_info = await fetch_all_movies(movies, semaphore)

        movie_data = [info for info in movie_info if info]
            
        # actor_data = [get_actor_info(url) for url in actors]

        print(f"Movies: {len(movie_data)}")
        movie_df = pd.DataFrame(movie_data)
        # actor_df = pd.DataFrame(actor_data)
        file_name = f'movies_{i}.csv'
        movie_df.to_csv(file_name, index=True)
        
        # actor_df.to_csv('actors.csv', index=False)

asyncio.run(get_data())
##IMDB Scraper

This is a simple web scraper that scrapes movies from IMDB and stores them in a CSV file. 
The scraper uses the BeautifulSoup library to parse the HTML content of the IMDB website and extract the relevant information about the movies.

The scraper extracts the following information about each movie:
- Title
- Year
- Rating
- Genres
- Director
- Writers
- Plot
- Age Classification
- Duration

The scraper can be run from the command line by running the `imdb_scraper.py` file. 

Adjust the following parameters in the `config.py` file to scrape the desired number of movies:

- START_ID : The ID of the first movie to scrape
- END_ID  : The ID of the last movie to scrape

- BATCH_SIZE : The number of movies to scrape in each batch  

- SEM_LIMIT : The number of concurrent requests to make to the IMDB website 

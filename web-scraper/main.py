import random
import requests
import csv 
from bs4 import BeautifulSoup

URL = 'http://www.imdb.com/chart/top'

def main():
    response = requests.get(URL)
    soup = BeautifulSoup(response.text, 'html.parser')
    movietags = soup.select('td.titleColumn')
    inner_movietags = soup.select('td.titleColumn a')
    ratingtags = soup.select('td.posterColumn span[name=ir]')

    def get_year(movie_tag):
        moviesplit = movie_tag.text.split()
        year = moviesplit[-1]
        return year

    years = [get_year(tag) for tag in movietags]
    actors_list =[tag['title'] for tag in inner_movietags] 
    titles = [tag.text for tag in inner_movietags]
    ratings = [float(tag['data-value']) for tag in ratingtags] 
    n_movies = len(titles)

    # with open("./names.csv", 'r') as file:
    #     csvreader = csv.reader(file)
    #     for row in csvreader:
    #         num = random.randint(0, 5)
    #         for i in range(num):

            
    #         print(random.randint(0, 250))
   
    for i in range(len(movietags)):
        print(f'{titles[i]}, {years[i]}, {ratings[i]}')

if __name__ == '__main__':
    main()


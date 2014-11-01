#*****************************************************************************
# Food encyclopedia scraper
#
#
#*****************************************************************************

from bs4 import BeautifulSoup
from urllib2 import urlopen
import unicodedata

BASE_URL = 'http://www.foodterms.com'

def make_soup(pageurl):
	html = urlopen(pageurl).read()
	return BeautifulSoup(html, 'lxml')

def get_food_links(pageurl):
	soup = make_soup(pageurl)
	arrows = soup.find_all('span', 'arrow')
	links = []
	for arrow in arrows:
		for link in arrow.find_all('a'):
			name = unicodedata.normalize('NFD',arrow.a.contents[0]) \
				.encode('ascii','ignore')
			links.append((arrow.a.contents[0], name, 
				BASE_URL + link.get('href')))
	return links

def get_descr(soup):
	tier3 = soup.find('div', 'tier-3')
	for tag in tier3.find_all('p'):
		if not tag.has_attr('class'):
			print tag.contents
			return tag.string
	return 'No description'

def get_syn(soup):
	seealso = soup.find('h6', 'seeAlso')
	if seealso is not None:
		return [tag.string for tag in seealso.find_next('ul').find_all('a')]
	return 'No Synonyms'
	
a = get_food_links('http://www.foodterms.com/encyclopedia/a/index.html')

for (_,_,link) in a:
	soup = make_soup(link)
	print get_descr(soup).encode('utf-8')
	print get_syn(soup).encode('utf-8')

#for letter in [chr(x) for x in xrange(ord('a'),ord('z')+1)]:
#	get_food_names(BASE_URL + letter + '/index.html')
	

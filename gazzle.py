import redis
import celery
from celery import current_app
from celery.contrib.methods import task_method
import time
import threading
import urllib2
import json
import re
from bs4 import BeautifulSoup


class Gazzle(object):
	def __init__(self, *args, **kwargs):
		self.redis   = redis.Redis(host="localhost", port=6379)
		self.sockets = []
		self.crawl_lock = threading.RLock()
		self.crawl_cond = threading.Condition()

		self._clear_crawl()

		crawl_thread = threading.Thread(target=self._crawl)
		crawl_thread.start()

	def add_socket(self, socket):
		self.sockets.append(socket)

	def remove_socket(self, socket):
		self.sockets.remove(socket)
	

	def _send_to_all(self, message):
		for socket in self.sockets:
			socket.write_message(message)

	def _clear_crawl(self):
		self.crawl_cond.acquire()		
		self.redis.delete('crawl:frontier')
		self.redis.delete('crawl:pagemap')
		self.redis.set('crawl:pagecount', 0)

		keys = self.redis.keys('crawl:pages*')
		for key in keys:
			self.redis.delete(key)

		self.crawl_cond.release()


	def _crawl(self):
		def extract_link(link, url):
			href = link.get('href', '')
			if href == '':
				return ''
			# if 'https://' in href:
			# 	return href.replace('https://', 'http://')
			if re.match('#.*', href) != None:
				return ''
			elif 'http' not in href:
				m = re.match('(http://[0-9a-zA-Z.]+)/*', url)
				return m.group(1) + href

		while True:
			self.crawl_cond.acquire()
			while self.redis.llen('crawl:frontier') == 0:
				self.crawl_cond.wait()

			item_index = int(self.redis.lpop('crawl:frontier'))
			item = self.redis.get('crawl:pages:%d:url' % item_index)


			page = urllib2.urlopen(item)
			soup = BeautifulSoup(page.read())

			title = soup.title.text
			self.redis.hset('crawl:pages:%d:title' % item_index, item, title)

			links = map(lambda link: extract_link(link, item), soup.find_all("a"))
			links = filter(lambda link: link != '' and link != None, links)
			links = filter(lambda link: self.redis.hexists('crawler:pagemap', item) == 0, links)

			print("Crawling %s found %d links" % (item, len(links)))
			result_links = []
			for link in links:
				index = self.redis.incr('craw:pagecount')
				self.redis.set('crawl:pages:%d:url' % index, link)
				self.redis.rpush('crawl:pages:%d:links' % item_index, index)
				self.redis.hset('crawl:pagemap', link, index)
				self.redis.rpush('crawl:frontier', index)
				result_links.append({'url': link, 'id': index})

			self.crawl_cond.release()

			res = [
				{
					'action': 'remove frontier',
					'items': [item_index],
				},
				{
					'action': 'add frontier',
					'items': result_links,
				},
			]

			self._send_to_all(json.dumps(res))




	def start_crawl(self, url=''):
		if url == '':
			url = 'http://en.wikipedia.org/wiki/Information_retrieval'

		
		self._clear_crawl()

		self.crawl_cond.acquire()
		
		self.redis.set('crawl:pagecount', 1)
		self.redis.set('crawl:pages:1:url', url)
		self.redis.hset('crawl:pagemap', url, 1)
		self.redis.rpush('crawl:frontier', 1)

		self.crawl_cond.notify()
		self.crawl_cond.release()
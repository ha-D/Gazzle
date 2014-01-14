import redis
import celery
from celery import current_app
from celery.contrib.methods import task_method
import time
import threading
import urllib2
import json
import re
from pymongo import MongoClient
from bs4 import BeautifulSoup
from Queue import Queue

class Gazzle(object):
	def __init__(self, *args, **kwargs):
		self.sockets = []

		mongo_client = MongoClient('localhost', 27017)
		self.mongo = mongo_client['gazzle']
		self.mongo.drop_collection('pages')
		self.pages = self.mongo['pages']

		self.frontier = Queue()
		self.crawlCount = 0
		self.crawling = False
		self.crawl_cond = threading.Condition()

		self._start_crawl_thread(count = 3)

		

	def add_socket(self, socket):
		self.sockets.append(socket)

	def remove_socket(self, socket):
		self.sockets.remove(socket)
	

	def _send_to_all(self, message):
		for socket in self.sockets:
			socket.write_message(message)

	def _start_crawl_thread(self, count = 1):
		for x in range(count):
			crawl_thread = threading.Thread(target=self._crawl, dae)
			crawl_thread.setDaemon(True)
			crawl_thread.start()	

	def _crawl(self):
		def extract_link(link, url):
			href = link.get('href', '')
			m = re.match('([^?]+)[?].*', unicode(href))
			if m != None:
				href = m.group(1)
			if href == '':
				return ''

			# if 'https://' in href:
			# 	return href.replace('https://', 'http://')
			if re.match('#.*', href) != None:
				return ''
			elif re.match('//.*', href):
				return 'http:' + href
			elif re.match('/.*', href):
				m = re.match('(http://[0-9a-zA-Z.]+)/*', url)
				# print("link %s %s going to %s" % (href, "",  ""))
				return m.group(1) + href


		while True:
			self.crawl_cond.acquire()
			while self.crawling:
				self.crawl_cond.wait()
			self.crawl_cond.release()

			item_index = self.frontier.get(True)
			item = self.pages.find_one({'page_id': item_index})
			self._send_to_all(json.dumps({
				'action': 'crawl current',
				'page': item['url']
			}))

			page = urllib2.urlopen(item['url'])
			soup = BeautifulSoup(page.read())

			title = soup.title.text
			body = soup.body.text
			links = map(lambda link: extract_link(link, item['url']), soup.find_all("a"))
			links = filter(lambda link: link != '' and link != None, links)
			links = filter(lambda link: link not in self.pageset, links)

			print("%s Crawling %s found %d links" % (threading.current_thread().name, item['url'], len(links)))

			result_links = []
			for link in links:
				page_id = len(self.pageset)
				self.pages.insert({
					'page_id': page_id,
					'url': link
				})
				self.pageset.add(link)
				self.frontier.put(page_id)
				result_links.append({'url': link, 'page_id': page_id})

			self.pages.update({'page_id': item_index}, {
				'$push': {'links': {'$each': result_links}},
				'$set':	{'title': title},
				'$set': {'content': body},
				'$set': {'crawled': True}
			})

			self.crawlCount += 1

			self._send_to_all(json.dumps([
				{
					'action': 'frontier size',
					'value': self.frontier.qsize()
				},
				{
					'action': 'crawl size',
					'value': self.crawlCount
				},
			]))




	def start_crawl(self, url=''):
		if url == '':
			url = 'http://en.wikipedia.org/wiki/Information_retrieval'

		self.pages.insert({
			'page_id': 0,
			'url': url,
			'crawled': False
		})	
		self.pageset = {url}
		self.frontier.put(0)

	def toggle_crawl(self):
		self.crawl_cond.acquire()
		self.crawling = not self.crawling
		self.crawl_cond.notifyAll()
		self.crawl_cond.release()

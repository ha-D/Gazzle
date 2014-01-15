from pymongo import MongoClient
from bs4 import BeautifulSoup
from Queue import Queue, LifoQueue
from whoosh.index import create_in
from whoosh.fields import *
import os, re, time, threading, urllib2, json

class Gazzle(object):
	def __init__(self, *args, **kwargs):
		self.sockets = []

		mongo_client = MongoClient('localhost', 27017)
		self.mongo = mongo_client['gazzle']
		self.mongo.drop_collection('pages')
		self.pages = self.mongo['pages']

		self._init_whoosh()

		self.frontier = Queue()
		self.crawlCount = 0
		self.crawling = False
		self.crawl_cond = threading.Condition()

		self.index_q = LifoQueue()
		self.indexing = False
		self.index_cond = threading.Condition()
		self.index_lock = threading.RLock()

		self.start_crawl_thread(count = 3)
		self.start_index_thread() # index writer doesn't support multithreading


	def _init_whoosh(self):
		schema = Schema(title=TEXT(stored=True), content=TEXT, url=ID(stored=True), rank=NUMERIC(stored=True, numtype=float))
		if not os.path.exists("index"):
	   		os.mkdir("index")
		self.index = create_in('index', schema)


	def _index(self):
		_ = {
			'lock': threading.RLock(),
			'writer': None,
			'need_commit': [],
		}

		def flush(_):
			while True:
				if len(_['need_commit']) != 0 and _['writer'] != None:
					_['lock'].acquire()
					_['writer'].commit()
					_['writer'] = None
					need_tmp = _['need_commit']
					_['need_commit'] = []
					_['lock'].release()
					self._send_to_all({
						'action': 'index commit',
						'pages': map(lambda x: {'page_id': x}, need_tmp)
					})
				time.sleep(5)

		self._start_thread(target = flush, kwargs={'_':_})

		while True:
			self.index_cond.acquire()
			while not self.indexing:
				self.index_cond.wait()
			self.index_cond.release()

			item_index = self.index_q.get(True)
			item = self.pages.find_one({'page_id': item_index})

			_['lock'].acquire()
			if _['writer'] == None:
				_['writer'] = self.index.writer()
			_['writer'].add_document(title=item['title'], content=item['content'], url=item['url'])
			_['need_commit'].append(item_index)
			_['lock'].release()

			self._send_to_all({
				'action': 'index page',
				'page': {'page_id': item_index}
			})


	def _crawl(self):
		while True:
			self.crawl_cond.acquire()
			while not self.crawling:
				self.crawl_cond.wait()
			self.crawl_cond.release()

			item_index = self.frontier.get(True)
			item = self.pages.find_one({'page_id': item_index})

			# self._send_to_all(json.dumps({
			# 	'action': 'crawl current',
			# 	'page': item['url']
			# }))

			page = urllib2.urlopen(item['url'])
			soup = BeautifulSoup(page.read())

			title = soup.title.text
			body = soup.body.text
			links = map(lambda link: self.extract_anchor_link(link, item['url']), soup.find_all("a"))
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
				'$set':	{'title': unicode(title), 'content': unicode(body), 'crawled': True}
			})

			self.crawlCount += 1
			self.index_q.put(item_index)

			self._send_to_all(json.dumps([
				{
					'action': 'crawl page',
					'page': {'page_id': item_index, 'url': item['url'], 'link_count': len(links), 'title': title}
				},
				{
					'action': 'frontier size',
					'value': self.frontier.qsize()
				},
				{
					'action': 'crawl size',
					'value': self.crawlCount
				},
			]))


	def extract_anchor_link(self, link, url):
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


	def _send_to_all(self, message):
		if type(message) != str:
			message = json.dumps(message)
		for socket in self.sockets:
			socket.write_message(message)

	def _start_thread(self, target, count=1, args=(), kwargs={}):
		for x in range(count):
			thread = threading.Thread(target=target, args=args, kwargs=kwargs)
			thread.setDaemon(True)
			thread.start()	

	def start_index_thread(self, count = 1):
		self._start_thread(target = self._index, count = count)

	def start_crawl_thread(self, count = 1):
		self._start_thread(target = self._crawl, count = count)

	def add_socket(self, socket):
		self.sockets.append(socket)

	def remove_socket(self, socket):
		self.sockets.remove(socket)

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
		self.toggle_crawl()

	def toggle_crawl(self):
		self.crawl_cond.acquire()
		self.crawling = not self.crawling
		self.crawl_cond.notifyAll()
		self.crawl_cond.release()

	def toggle_index(self, state=None):
		self.index_cond.acquire()
		if state == None:
			self.indexing = not self.indexing
		else:
			self.indexing = state
		self.index_cond.notifyAll()
		self.index_cond.release()

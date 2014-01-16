from pymongo import MongoClient
from bs4 import BeautifulSoup
from Queue import Queue, LifoQueue
from whoosh.index import create_in, open_dir
from whoosh.fields import *
from whoosh.qparser import QueryParser
import os, re, time, threading, urllib2, json

class Gazzle(object):
	def __init__(self, *args, **kwargs):
		self.sockets = []

		mongo_client = MongoClient('localhost', 27017)
		self.mongo = mongo_client['gazzle']
		# self.mongo.drop_collection('pages')
		self.pages = self.mongo['pages']

		self._init_whoosh()

		self.pageset = set()
		self.frontier = Queue()
		self.crawlCount = 0
		self.crawling = False
		self.crawl_cond = threading.Condition()
		self.crawl_lock = threading.RLock()
		self._init_crawl()

		self.index_set = set()
		self.index_q = LifoQueue()
		self.index_altq = LifoQueue()
		self.index_alt_switchoff = False
		self.indexing = False
		self.index_cond = threading.Condition()
		self.index_lock = threading.RLock()
		self._init_index()

		self.start_crawl_thread(count = 3)
		self.start_index_thread() # index writer doesn't support multithreading


	def _init_crawl(self):
		self.pageset = set()
		self.frontier = Queue()
		for page in self.pages.find():
			self.pageset.add(page['url'])
		for page in self.pages.find({'crawled': False}):
			self.frontier.put(page)
		self.crawlCount = self.pages.find({'crawled': True}).count()
		print('Added %d pages to page set' % len(self.pageset))
		print('Added %d pages to frontier' % self.frontier.qsize())
		print('Crawl count set to %d' % self.crawlCount)

	def _init_index(self):
		self.index_set = set()
		self.index_q = LifoQueue()
		for page in self.pages.find({'indexed': True}):
			self.index_set.add(page['page_id'])
		for page in self.pages.find({'crawled':True, 'indexed': False}):
			self.index_q.put(page['page_id'])
		print('Added %d pages to index set' % len(self.index_set))
		print('Added %d pages to index queue' % self.index_q.qsize())

	def _init_whoosh(self, clear = False):
		schema = Schema(page_id=STORED, title=TEXT(stored=True), content=TEXT, url=ID(stored=True), rank=NUMERIC(stored=True, numtype=float))
		if not os.path.exists("index"):
	   		os.mkdir("index")
	   		clear = True
	   	if clear:
			self.index = create_in('index', schema)
		else:
			self.index = open_dir("index")

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
					self.pages.update({'page_id' : {'$in': need_tmp}}, {'$set':	{'indexed': True}}, multi = True, upsert = False)
				time.sleep(5)

		self._start_thread(target = flush, kwargs={'_':_})

		while True:
			self.index_cond.acquire()
			while not self.indexing:
				self.index_cond.wait()
			self.index_cond.release()

			try:
				item_index = self.index_altq.get(False)
				if self.index_alt_switchoff:
					self.indexing = False
			except:
				item_index = self.index_q.get(True)
			
			if item_index in self.index_set:
				continue

			item = self.pages.find_one({'page_id': item_index})

			_['lock'].acquire()
			if _['writer'] == None:
				_['writer'] = self.index.writer()
			_['writer'].add_document(page_id=item_index, title=item['title'], content=item['content'], url=item['url'])
			_['need_commit'].append(item_index)
			_['lock'].release()

			self.index_set.add(item_index)
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

			page = urllib2.urlopen(item['url'])
			soup = BeautifulSoup(page.read())

			title = soup.title.text.replace(' - Wikipedia, the free encyclopedia', '')
			body  = soup.body.text
			links = map(lambda link: self.extract_anchor_link(link, item['url']), soup.find_all("a"))
			links = filter(lambda link: link != '' and link != None, links)

			self.crawl_lock.acquire()

			links = filter(lambda link: link not in self.pageset, links)

			print("%s Crawling %s found %d links" % (threading.current_thread().name, item['url'], len(links)))

			result_links = []
			for link in links:
				page_id = len(self.pageset)
				self.pages.insert({
					'page_id': page_id,
					'url': link,
					'crawled': False,
					'indexed': False
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

			self.crawl_lock.release()

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

	def search(self, socket, query):
		with self.index.searcher() as searcher:
			parser = QueryParser("content", self.index.schema)
			parsed_query = parser.parse(query)
			results = searcher.search(parsed_query)

			results = map(lambda x: dict(x), results)
			print(results)
			socket.write_message(json.dumps({
				'action': 'search results',
				'results' : results
			}))

	def clear_index(self):
		self._init_whoosh(clear = True)
		self.pages.update({'indexed': True}, {'$set': {'indexed': False}}, multi = True, upsert = False)
		self._init_index()
		self._send_to_all({
			'action': 'index clear'
		})

	def clear_frontier(self):
		self.pages.remove({'crawled': False})
		self._init_crawl()
		self._send_to_all({
			'action': 'init',
			'frontier_size': 0
		})

	def clear_all(self):
		self.mongo.drop_collection('pages')
		self._init_whoosh(clear = True)
		self._init_index()
		self._init_crawl()
		self.indexing = False
		self.crawling = False
		self._send_to_all(json.dumps({
			'action': 'init',
			'pages': [],
			'frontier_size': 0,
			'crawl_size': 0,
			'crawling': False,
			'indexing': False
		}))	

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
		pages = self.pages.find({'crawled': True}, {'_id': False, 'page_id':True, 'url': True, 'title': True, 'indexed': True})
		pages = map(lambda x: {'page_id': x['page_id'], 'title': x['title'], 'url': x['url'], 'indexed': x['indexed']}, pages)
		socket.write_message(json.dumps({
			'action': 'init',
			'pages': pages,
			'frontier_size': self.frontier.qsize(),
			'crawl_size': self.crawlCount,
			'crawling': self.crawling,
			'indexing': self.indexing
		}))

	def remove_socket(self, socket):
		self.sockets.remove(socket)

	def start_crawl(self, url=''):
		if url == '':
			url = 'http://en.wikipedia.org/wiki/Information_retrieval'

		self.pages.insert({
			'page_id': len(self.pageset),
			'url': url,
			'crawled': False,
			'indexed': False
		})	
		self.frontier.put(len(self.pageset))
		self.pageset.add(url)
		self.toggle_crawl(state = True)

	def toggle_crawl(self, state=None):
		self.crawl_cond.acquire()
		if state == None:
			self.crawling = not self.crawling
		else:
			self.crawling = state
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

	def index_page(self, page):
		self.index_altq.put(page)
		self.index_cond.acquire()
		self.index_alt_switchoff = not self.indexing
		self.indexing = True
		self.index_cond.notifyAll()
		self.index_cond.release()		

from pymongo 		import MongoClient
from bs4 			import BeautifulSoup
from Queue 			import Queue, LifoQueue
from whoosh.index 	import create_in, open_dir
from whoosh.fields 	import *
from whoosh.qparser import QueryParser
from numpy 			import dot
import os, re, time, threading, urllib2, json, math

class Gazzle(object):
	def __init__(self, *args, **kwargs):
		self.sockets = []

		mongo_client = MongoClient('localhost', 27017)
		self.mongo = mongo_client['gazzle']
		# self.mongo.drop_collection('pages')
		self.pages = self.mongo['pages']

		self._init_whoosh()

		self.pageset = {}
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

		self.pagerank_cond = threading.Condition()
		self._start_thread(target = self._crawl, count = 3)
		self._start_thread(target = self._index, count = 1) # index writer doesn't support multithreading
		self._start_thread(target = self._pagerank, count = 1)
		self._start_thread(target = self._assert_thread, count=1)


	def _init_crawl(self):
		self.pageset = {}
		self.frontier = Queue()
		for page in self.pages.find():
			self.pageset[page['url']] = page['page_id']
		for page in self.pages.find({'crawled': False}):
			self.frontier.put(page['page_id'])
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


	def _assert_thread(self):
		while True:
			a = self.pages.find_one({'crawled': True, 'title': {'$exists': False}})
			assert a == None, 'Found inconsistent page in db ID: %d URL: %s' % (a['page_id'], a['url'])
			time.sleep(1)

	def _pagerank(self):
		while True:
			with self.pagerank_cond:
				self.pagerank_cond.wait()
			pages = self.pages.find({'crawled': True, 'indexed': True}, {
				'_id':False,
				'content': False,
				'links.url': False
			})

			RANK_SCALE = 1
			ALPHA = 0.25

			page_count = pages.count()

			id_to_ind = {}
			ind_to_id = []
			for page in pages:
				ind = len(id_to_ind)
				ind_to_id.append(page['page_id'])
				id_to_ind[page['page_id']] = ind

			pages.rewind()
			pmat = []
			for page in pages:
				row = [0.0] * page_count
				link_count = 0
				for link in page['links']:
					if link['page_id'] in id_to_ind:
						ind = id_to_ind[link['page_id']]
						row[ind] += RANK_SCALE
						link_count += 1
				alph = ALPHA * RANK_SCALE / page_count
				for ind in range(page_count):
					if link_count == 0:
						row[ind] += 1 / page_count
					else:
						row[ind] *= (1 - alph) / link_count
						row[ind] += alph / page_count
				pmat.append(row)

			page_rank = [0] * page_count
			page_rank[0] = 1
			for d in range(30):
				page_rank = dot(page_rank, pmat)

			result = [{"page_id": ind_to_id[x], "rank": self._format_rank(page_rank[x])} for x in range(page_count)]

			self._send_to_all({
				'action': 'page rank',
				'pages': result
			})

			for ind in range(page_count):
				self.pages.update({"page_id": ind_to_id[ind]}, {"$set": {"rank": page_rank[ind]}}, upsert=False)

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
					with self.pagerank_cond:
						self.pagerank_cond.notify()
				time.sleep(5)

		self._start_thread(target = flush, kwargs={'_':_})

		while True:
			with self.index_cond:
				while not self.indexing:
					self.index_cond.wait()
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

			assert item.get('title') != None , 'Uncrawled page in index queue, ID: %d, URL: %s' %(item['page_id'], item['url'])
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
			with self.crawl_cond:
				while not self.crawling:
					self.crawl_cond.wait()

			item_index = self.frontier.get(True)
			item = self.pages.find_one({'page_id': item_index})

			page = urllib2.urlopen(item['url'])
			soup = BeautifulSoup(page.read())

			title = soup.title.text.replace(' - Wikipedia, the free encyclopedia', '')
			body  = soup.body.text
			links = map(lambda link: self.extract_anchor_link(link, item['url']), soup.find_all("a"))
			links = filter(lambda link: link != '' and link != None, links)

			with self.crawl_lock:
				# links = filter(lambda link: link not in self.pageset, links)

				print("%s Crawling %s found %d links" % (threading.current_thread().name, item['url'], len(links)))

				result_links = []
				for link in links:
					if link not in self.pageset:
						page_id = len(self.pageset)
						self.pages.insert({
							'page_id': page_id,
							'url': link,
							'crawled': False,
							'indexed': False
						})
						self.pageset[link] = page_id
						self.frontier.put(page_id)
					else:
						page_id = self.pageset[link]
					result_links.append({'url': link, 'page_id': page_id})


				self.crawlCount += 1
				self.index_q.put(item_index)

			self.pages.update({'page_id': item_index}, {
				'$push': {'links': {'$each': result_links}},
				'$set':	{'title': unicode(title), 'content': unicode(body), 'crawled': True}
			})

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



	def search(self, socket, query, rank_part=0):
		def sort_results(results):
			scores = {}
			max_score = 0
			max_rank = 0
			for res in results:
				scores[res.fields()['page_id']] = res.score
				if res.score > max_score: max_score = res.score

			page_ids = map(lambda x: x.fields()['page_id'], results)
			pages = self.pages.find({"page_id": {"$in": page_ids}}, {"title": True, "page_id":True, "rank":True, "url": True})
			pages = map(lambda x: dict(x), pages)
			for page in pages:
				if 'rank' not in page:
					page['rank'] = 0
				if page['rank'] > max_rank:
					max_rank = page['rank']

			for page in pages:
				del page['_id']
				rank = 1 - page['rank'] / float(max_rank)
				score = scores[page['page_id']] / float(max_score)

				final_score = rank * (rank_part / 100.0) + score * (1 - rank_part / 100.0)
				page['score'] = final_score

			pages.sort(key = lambda x: x['score'])
			return pages


		with self.index.searcher() as searcher:
			parser = QueryParser("content", self.index.schema)
			parsed_query = parser.parse(query)
			results = searcher.search(parsed_query)

			if len(results) > 0:
				print("found some")
				print(len(results))
				results = sort_results(results)
			else:
				results = []

			# results = map(lambda x: dict(x), results)
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

	def _format_rank(self, rank):
		if rank == None:
			return None
		return "%.2f" % (math.log(rank + 1) * 1000)

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


	def add_socket(self, socket):
		self.sockets.append(socket)
		pages = self.pages.find({'crawled': True}, {'_id': False, 'page_id':True, 'url': True, 'title': True, 'indexed': True, 'rank': True})
		pages = map(lambda x: {'page_id': x['page_id'], 'title': x['title'], 'url': x['url'], 'indexed': x['indexed'], 'rank': self._format_rank(x.get('rank'))}, pages)
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

		with self.crawl_lock:
			page_id =  len(self.pageset)
			self.pages.insert({
				'page_id': page_id,
				'url': url,
				'crawled': False,
				'indexed': False
			})	
			self.frontier.put(len(self.pageset))
			self.pageset[url] = page_id
		self.toggle_crawl(state = True)

	def toggle_crawl(self, state=None):
		with self.crawl_cond:
			if state == None:
				self.crawling = not self.crawling
			else:
				self.crawling = state
			self.crawl_cond.notifyAll()
		self._send_to_all({
			'action': 'init',
			'crawling': self.crawling
		})

	def toggle_index(self, state=None):
		with self.index_cond:
			if state == None:
				self.indexing = not self.indexing
			else:
				self.indexing = state
			self.index_cond.notifyAll()
		self._send_to_all({
			'action': 'init',
			'indexing': self.indexing
		})

	def index_page(self, page):
		self.index_altq.put(page)
		with self.index_cond:
			self.index_alt_switchoff = not self.indexing
			self.indexing = True
			self.index_cond.notifyAll()

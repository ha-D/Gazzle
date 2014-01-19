import tornado.httpserver
import tornado.websocket
import tornado.ioloop
import tornado.web
import tornado.autoreload
import json
from datetime import datetime
from gazzle import Gazzle

class WSHandler(tornado.websocket.WebSocketHandler):
    def open(self):
        print('connection opened')
        gazzle.add_socket(self)
      
    def on_message(self, message):
        print(message)
        # gazzle.start_crawl()
        mes = json.loads(message)
        if mes.get('action') == 'start crawl':
            url = mes.get('page', '')
            gazzle.start_crawl(url = url)
        elif mes.get('action') == 'toggle crawl':
            gazzle.toggle_crawl()
        elif mes.get('action') == 'start index':
            gazzle.toggle_index(state=True)
        elif mes.get('action') == 'stop index':
            gazzle.toggle_index(state=False)
        elif mes.get('action') == 'index page':
            gazzle.index_page(mes['page'])
        elif mes.get('action') == 'clear index':
            gazzle.clear_index()
        elif mes.get('action') == 'clear frontier':
            gazzle.clear_frontier()
        elif mes.get('action') == 'clear all':
            gazzle.clear_all()
        elif mes.get('action') == 'search':
            gazzle.search(socket = self, query = mes['query'])
 
    def on_close(self):
        gazzle.remove_socket(self)
        print 'connection closed'
 
application = tornado.web.Application([
    (r'/ws', WSHandler),
])
 
if __name__ == "__main__":
    gazzle = Gazzle()

    port = 3300
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(port)

    print("%s \tRunning on port %d" % (datetime.now(), port))

    io_loop = tornado.ioloop.IOLoop.instance()
    tornado.autoreload.start(io_loop = io_loop, check_time=5000)
    io_loop.start()
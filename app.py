import tornado.httpserver
import tornado.websocket
import tornado.ioloop
import tornado.web
import tornado.autoreload
from datetime import datetime
from gazzle import Gazzle
import json, sys

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
            gazzle.search(socket = self, query = mes['query'], rank_part= mes.get('rank', 0))
        elif mes.get('action') == 'crossdomain crawl':
            gazzle.toggle_crosssite_crawl(state = mes.get('value'))
 
    def on_close(self):
        gazzle.remove_socket(self)
        print 'connection closed'
 
class MainHandler(tornado.web.RequestHandler):
    def get(self, x = None):
        self.render("public/index.html")

application = tornado.web.Application([
    (r'/ws', WSHandler),
    # (r'/favicon.ico', tornado.web.StaticFileHandler, {'path': favicon_path}),
    (r'/static/(.*)', tornado.web.StaticFileHandler, {'path': 'public/static/'}),
    (r'/(.*)', MainHandler)
])
 
if __name__ == "__main__":
    port = 3300
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except:
            pass

    # print("%s \tRunning on port %d" % (datetime.now(), port))
    print("Running on port %d" % port)

    gazzle = Gazzle(crawl_threads=3)

    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(port)

    io_loop = tornado.ioloop.IOLoop.instance()
    tornado.autoreload.start(io_loop = io_loop, check_time=5000)
    io_loop.start()

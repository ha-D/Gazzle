import tornado.httpserver
import tornado.websocket
import tornado.ioloop
import tornado.web

import json
from gazzle import Gazzle

class WSHandler(tornado.websocket.WebSocketHandler):
    def open(self):
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
 
    def on_close(self):
        gazzle.remove_socket(self)
        print 'connection closed'
 
application = tornado.web.Application([
    (r'/ws', WSHandler),
])
 
if __name__ == "__main__":
    gazzle = Gazzle()

    port = 8880
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(port)

    print("Running on port %d" % (port))

    tornado.ioloop.IOLoop.instance().start()
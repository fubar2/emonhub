"""class EmonHubTabularInterfacer
"""
import time
import json
from pydispatch import dispatcher
from emonhub_interfacer import EmonHubInterfacer

class EmonHubTabularInterfacer(EmonHubInterfacer):

    def __init__(self, name):
        # Initialization
        super(EmonHubTabularInterfacer, self).__init__(name)
        
        self._name = name
        
        self._settings = {
            'outpath': "/home/pi/test_tabular.xls",
            'outformat': "tabular",
            'senddata': 1,
            'sendstatus': 0,
            'subchannels':['ch3'],
            'pubchannels':['ch4'],
        }
        
        self.buffer = []
        self.lastsent = time.time() 
        self.lastsentstatus = time.time()

    def receiver(self, cargo):
        # Create a frame of data in "emonCMS format"
        f = []
        f.append(int(cargo.timestamp))
        f.append(cargo.nodeid)
        for i in cargo.realdata:
            f.append(i)
        if cargo.rssi:
            f.append(cargo.rssi)

        self._log.debug(str(cargo.uri) + " adding frame to buffer => "+ str(f))
        # Append to bulk post buffer
        self.buffer.append(f)
        
    def action(self):
    
        now = time.time()
        
        if (now-self.lastsent)>30:
            self.lastsent = now
            # print json.dumps(self.buffer)
            if int(self._settings['senddata']):
                self.bulkpost(self.buffer)
            self.buffer = []
            
        if (now-self.lastsentstatus)>60:
            self.lastsentstatus = now
            if int(self._settings['sendstatus']):
                self.sendstatus()
            
    def bulkpost(self,databuffer):
    
        data_string = json.dumps(databuffer, separators=(',', ':'))
        ds = ['\t'.join([str(y) for y in x]) for x in eval(data_string)]
    post_body = '\n'.join(ds)

        # time that the request was sent at
        sentat = int(time.time())

        # logged before apikey added for security
        self._log.info("sending: " + post_body)
        if (post_body.endswith('\n') == False):
              post_body = '%s\n' % post_body
        reply = self._send_post(post_body)
        if reply == 'ok':
            self._log.debug("acknowledged receipt with '" + reply + "' from writing to" + self._settings['outpath'])
            return True
        else:
            self._log.warning("send failure: wanted 'ok' but got '" +reply+ "'")
            
    def _send_post(self, post_body=None):
        """

        :param post_body:
        :return: the received reply if request is successful
        """
        """Send data to server.

        data (list): node and values (eg: '[node,val1,val2,...]')
        time (int): timestamp, time when sample was recorded

        return True if data sent correctly

        """

        reply = ""
        fn = self._settings['outpath']
        try:
        f = open(fn,'a')
            f.write(post_body)
            f.close()
            reply = 'ok'
        except Exception:
            import traceback
            self._log.warning(self.name + " couldn't write to file" + fn +  " Exception: " +
                              traceback.format_exc())
            reply = ""
        finally:
            return reply
            
    def sendstatus(self):
        reply = self._send_post(None)
            
    def set(self, **kwargs):
        for key,setting in self._settings.iteritems():
            if key in kwargs.keys():
                # replace default
                self._settings[key] = kwargs[key]
        
        # Subscribe to internal channels
        for channel in self._settings["subchannels"]:
            dispatcher.connect(self.receiver, channel)
            self._log.debug(self._name+" Subscribed to channel' : " + str(channel))



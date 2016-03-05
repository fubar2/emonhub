"""class EmonHubRateLimitedEmoncmsHTTPInterfacer
"""
import time
import json
import urllib2
import httplib
from pydispatch import dispatcher
from emonhub_interfacer import EmonHubInterfacer

"""
2016-03-05 17:04:36,285 DEBUG    emoncmsorg ### downsampling [[1457157847, 5, 743, 0, 743, 234.9, 27.5, 0, 0, 0, 0, 0, 1], [1457157851, 5, 747, 0, 747, 235.84, 27.5, 0, 0, 0, 0, 0, 1], [1457157856, 5, 742, 0, 742, 234.88, 27.5, 0, 0, 0, 0, 0, 1], [1457157861, 5, 743, 0, 743, 235.26, 27.5, 0, 0, 0, 0, 0, 1], [1457157866, 5, 755, 0, 755, 236.32, 27.5, 0, 0, 0, 0, 0, 1], [1457157871, 5, 739, 0, 739, 234.64000000000001, 27.5, 0, 0, 0, 0, 0, 1], [1457157876, 5, 743, 0, 743, 235.47, 27.5, 0, 0, 0, 0, 0, 1]]
2016-03-05 17:04:36,288 INFO     emoncmsorg sending: http://emoncms.org/input/bulk.json?apikey=E-M-O-N-C-M-S-A-P-I-K-E-Y&data=[[["1457157849","5","745","0","745","234","27","0","0","0","0","0","1"]],[["1457157856","5","742","0","742","234","27","0","0","0","0","0","1"]],[["1457157861","5","743","0","743","235","27","0","0","0","0","0","1"]],[["1457157866","5","755","0","755","236","27","0","0","0","0","0","1"]],[["1457157873","5","741","0","741","234","27","0","0","0","0","0","1"]]]&sentat=1457157876
2016-03-05 17:04:37,129 DEBUG    emoncmsorg acknowledged receipt with 'ok' from http://emoncms.org

"""


class EmonHubRateLimitedEmoncmsHTTPInterfacer(EmonHubInterfacer):

    def __init__(self, name):
        # Initialization
        super(EmonHubRateLimitedEmoncmsHTTPInterfacer, self).__init__(name)
        
        self._name = name
        
        self._settings = {
            'subchannels':['ch1'],
            'pubchannels':['ch2'],
            
            'apikey': "",
            'url': "http://emoncms.org",
            'senddata': 1,
            'sendstatus': 0,
            'downsamplesec' : 5 # rate throttle
        }
        
        self.buffer = []
        self.lastsent = time.time() 
        self.lastsentstatus = time.time()

    def downsample(self,rawd,sampleInt):

        def get_means(inrows):
            """ supply rows accumulated during a sampling interval and
            get back column means for each node's data INCLUDING TIME - seems sensible.. but ymmv
            """
            res = []
            nodes = set([x[1] for x in inrows])
            for node in list(nodes):
                subset = [x for x in inrows if x[1] == node]
                rowl = len(subset[0])
                nrow = float(len(subset))
                sums = [0 for i in range(rowl)]
                for row in subset:
                    for i,val in enumerate(row):
                        sums[i] += int(val)
                means = [int(x/nrow) for x in sums]
                res.append(means)
            return res

        rawd.sort() # time
	self._log.debug('### downsampling %s' % str(rawd)) 
        # time sorted just in case
        # down sample to sampleInt by taking mean of the set of data rows
        # for that sampleInt interval
        rowl = len(rawd[0])
        nrow = len(rawd)
        lastrow = nrow - 1  # index offset for zero base
        row0 = rawd[0]
        
        t0 = float(row0[0])
        nextdue = t0 + sampleInt
        nextsample = []
        outdat = []
        for i,row in enumerate(rawd):
            t = float(row[0])
            if i == lastrow: # stop on last row
                nextsample.append(row) # special case
            if t >= nextdue or i == lastrow: 
                nextdue = t + sampleInt
                mdat = get_means(nextsample)
                outdat.append(mdat)
                nextsample = []
            if i < lastrow:
                nextsample.append(row) # more to do
        lns = len(nextsample) # 
        if lns > 0:
             s = '# last sample has %d rows in bulkpost' % lns
             self._log.debug(s)
             mdat = get_means(nextsample)
             outdat.append(mdat)
        return outdat




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
            # print json.dumps(self.buffer)
            if int(self._settings['senddata']):
                self.bulkpost(self.buffer)
            self.buffer = []
            self.lastsent = now
            
        if (now-self.lastsentstatus)>60:
            self.lastsentstatus = now
            if int(self._settings['sendstatus']):
                self.sendstatus()
            
    def bulkpost(self,databuffer): 

            
        if not 'apikey' in self._settings.keys() or str.__len__(str(self._settings['apikey'])) != 32 \
                or str.lower(str(self._settings['apikey'])) == 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx':
            return
        sampleInt = float(self._settings['downsamplesec'])
        if len(databuffer) == 0:
            self._log.debug("##in bulkpost: zero rows in databuffer")
        if (time.time() - self.lastsent) >= 2*sampleInt:
            outdat = self.downsample(databuffer,sampleInt)
            data_string = json.dumps(outdat,separators=(',', ':'))
        else: # no need to downsample
            data_string = json.dumps(databuffer,separators=(',', ':'))
        # Prepare URL string of the form
        # http://domain.tld/emoncms/input/bulk.json?apikey=12345
        # &data=[[0,10,82,23],[5,10,82,23],[10,10,82,23]]
        # &sentat=15' (requires emoncms >= 8.0)
       
        sentat = int(time.time())

        # Construct post_url (without apikey)
        post_url = self._settings['url']+'/input/bulk'+'.json?apikey='
        post_body = "data="+data_string+"&sentat="+str(sentat)

        # logged before apikey added for security
        self._log.info("sending: " + post_url + "E-M-O-N-C-M-S-A-P-I-K-E-Y&" + post_body)

        # Add apikey to post_url
        post_url = post_url + self._settings['apikey']

        # The Develop branch of emoncms allows for the sending of the apikey in the post
        # body, this should be moved from the url to the body as soon as this is widely
        # adopted

        reply = self._send_post(post_url, post_body)
        if reply == 'ok':
            self._log.debug("acknowledged receipt with '" + reply + "' from " + self._settings['url'])
            return True
        else:
            self._log.warning("send failure: wanted 'ok' but got '" +reply+ "'")


            
    def _send_post(self, post_url, post_body=None):
        """

        :param post_url:
        :param post_body:
        :return: the received reply if request is successful
        """
        """Send data to server.

        data (list): node and values (eg: '[node,val1,val2,...]')
        time (int): timestamp, time when sample was recorded

        return True if data sent correctly

        """

        reply = ""
        request = urllib2.Request(post_url, post_body)
        try:
            response = urllib2.urlopen(request, timeout=60)
        except urllib2.HTTPError as e:
            self._log.warning(self.name + " couldn't send to server, HTTPError: " +
                              str(e.code))
        except urllib2.URLError as e:
            self._log.warning(self.name + " couldn't send to server, URLError: " +
                              str(e.reason))
        except httplib.HTTPException:
            self._log.warning(self.name + " couldn't send to server, HTTPException")
        except Exception:
            import traceback
            self._log.warning(self.name + " couldn't send to server, Exception: " +
                              traceback.format_exc())
        else:
            reply = response.read()
        finally:
            return reply
            
    def sendstatus(self):
        if not 'apikey' in self._settings.keys() or str.__len__(str(self._settings['apikey'])) != 32 \
                or str.lower(str(self._settings['apikey'])) == 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx':
            return
        
        # MYIP url
        post_url = self._settings['url']+'/myip/set.json?apikey='
        # Print info log
        self._log.info("sending: " + post_url + "E-M-O-N-C-M-S-A-P-I-K-E-Y")
        # add apikey
        post_url = post_url + self._settings['apikey']
        # send request
        reply = self._send_post(post_url,None)
            
    def set(self, **kwargs):
        for key,setting in self._settings.iteritems():
            if key in kwargs.keys():
                # replace default
                self._settings[key] = kwargs[key]
        
        # Subscribe to internal channels
        for channel in self._settings["subchannels"]:
            dispatcher.connect(self.receiver, channel)
            self._log.debug(self._name+" Subscribed to channel' : " + str(channel))


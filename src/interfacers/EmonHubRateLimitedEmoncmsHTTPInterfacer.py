"""class EmonHubRateLimitedEmoncmsHTTPInterfacer
"""
import time
import json
import urllib2
import httplib
from pydispatch import dispatcher
from emonhub_interfacer import EmonHubInterfacer

"""
2016-03-05 17:41:05,941 DEBUG    emoncmsorg ### downsampling [[1457160036, 5, 821, 0, 821, 237.12, 26.900000000000002, 0, 0, 0, 0, 0, 1], [1457160041, 5, 820, 0, 820, 237.20000000000002, 26.900000000000002, 0, 0, 0, 0, 0, 1], [1457160046, 5, 821, 0, 821, 237.24, 26.900000000000002, 0, 0, 0, 0, 0, 1], [1457160051, 5, 818, 0, 818, 236.76, 26.900000000000002, 0, 0, 0, 0, 0, 1], [1457160056, 5, 817, 0, 817, 237.33, 26.900000000000002, 0, 0, 0, 0, 0, 1], [1457160061, 5, 822, 0, 822, 236.98000000000002, 26.900000000000002, 0, 0, 0, 0, 0, 1]]
2016-03-05 17:41:05,944 DEBUG    emoncmsorg # last sample has 1 rows in bulkpost
2016-03-05 17:41:05,945 INFO     emoncmsorg sending: http://emoncms.org/input/bulk.json?apikey=E-M-O-N-C-M-S-A-P-I-K-E-Y&data=[[[1457160036,5,821,0,821,237,26,0,0,0,0,0,1]],[[1457160041,5,820,0,820,237,26,0,0,0,0,0,1]],[[1457160046,5,821,0,821,237,26,0,0,0,0,0,1]],[[1457160051,5,818,0,818,236,26,0,0,0,0,0,1]],[[1457160056,5,817,0,817,237,26,0,0,0,0,0,1]],[[1457160061,5,822,0,822,236,26,0,0,0,0,0,1]]]&sentat=1457160065

this works:
2016-03-06 13:06:01,712 DEBUG    RFM2Pi     954 Sent to channel' : ToEmonCMS
2016-03-06 13:06:06,034 INFO     emoncmsorg sending: http://emoncms.org/input/bulk.json?apikey=E-M-O-N-C-M-S-A-P-I-K-E-Y&data=[[1457229941,5,651,0,651,235.86,28.200000000000003,0,0,0,0,0,1],[1457229946,5,647,0,647,235.88,28.200000000000003,0,0,0,0,0,1],[1457229951,5,655,0,655,235.6,28.200000000000003,0,0,0,0,0,1],[1457229956,5,644,0,644,235.67000000000002,28.200000000000003,0,0,0,0,0,1],[1457229961,5,649,0,649,235.51,28.1,0,0,0,0,0,1]]&sentat=1457229966



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
                if (len(nodes) > 1):
                    subset = [x for x in inrows if x[1] == node]
                else:
                    subset = inrows # only one node
                rowl = len(subset[0])
                nrow = float(len(subset))
                sums = [0 for i in range(rowl)]
                for row in subset:
                    for i,val in enumerate(row):
                        sums[i] += float(val)
                means = [x/nrow for x in sums]
                res.append(means)
            #if (len(res) == 1):
            #    res = res[0] # get rid of outer list
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
            if t >= nextdue:
                nextdue = t + sampleInt
                mdat = get_means(nextsample)
                for m in mdat:
                    outdat.append(m)
                nextsample = []
            nextsample.append(row) # more to do
        lns = len(nextsample) # 
        if lns > 0:
             s = '# last sample has %d rows in bulkpost' % lns
             self._log.debug(s)
             mdat = get_means(nextsample)
             outdat.append(mdat)
        self._log.debug('### downsample returning %s' % str(outdat)) 
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
            data_string = str(outdat)
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
        self._log.info("rate limited sending: " + post_url + "E-M-O-N-C-M-S-A-P-I-K-E-Y&" + post_body)

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


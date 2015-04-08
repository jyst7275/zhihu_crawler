#encoding = utf-8
#decoding = utf-8
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import BitVector
import threading,re,json,time,pymysql
from collections import deque
list_lock = threading.Lock()
class dbController:
	def __init__(self,user,passwd,dbname):
			self.conn = pymysql.connect(user = user,passwd = passwd,host = "localhost",db = dbname)
			self.cur = self.conn.cursor()
			self.question_ex = "insert question values(%s,%s,%s,%s)"
			self.answer_ex = "insert answer values(%s,%s,%s,%s,%s)"
			self.people_ex = "insert people values(%s,%s,%s,%s,%s,%s,%s,%s)"
			self.label_ex = "insert label values(%s,%s)"
			self.people_follow = "insert people_follow values(%s,%s)"
			self.conn.set_charset('utf8')
			self.f = open("db.log","w",encoding="utf-8")
	def __del__(self):
		pass
	def db_people(self,peo_id,asks,thanks,agree,answers,followees,followers,topics):
		try:
			self.cur.execute(self.people_ex,(peo_id,asks,thanks,agree,answers,followees,followers,topics))
		except:
			self.f.write("error in people")
	def db_label(self,label,question_id):
		try:
			self.cur.execute(self.label_ex,(label,question_id))
		except:
			self.f.write("error in label")
	def db_answer(self,answer_id,agree,people_id,content,question_id):
		try:
			self.cur.execute(self.answer_ex,(answer_id,agree,people_id,content,question_id))
		except:
			self.f.write("error in answer")
	def db_question(self,question_id,follow,head,content):
		try:
			self.cur.execute(self.question_ex,(question_id,follow,head,content))
		except:
			self.f.write("error in question")
	def db_people_follow(self,people_a,people_b):
		try:
			self.cur.execute(self.people_follow,(people_a,people_b))
		except:
			self.f.write("error in follow")
	def db_commit(self):
		self.conn.commit()
class crawler:
	def __init__(self,dbcon):
		self.dbcon = dbcon;
		self.http = requests.session()
		# self.f = open('crawler.log','w+',encoding='utf-8')
		self.pattern_peo = re.compile(r'http://www.zhihu.com/people/[^/]+')
		self.pattern_que = re.compile(r'http://www.zhihu.com/question/[\d]+')
		self.pattern_in = re.compile(r'http://www.zhihu.com')
		self.pattern_forbi = re.compile(r'http://www.zhihu.com/logout|http://www.zhihu.com/people/[\w]{32}|http://www.zhihu.com/people/edit')
		self.pattern_answer = re.compile(r'http://www.zhihu.com/question/[\d]+/answer/[\d]+')
	def __del__(self):
		pass

	def login(self):
		self.header = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.89 Safari/537.36'}
		r = self.http.get('http://www.zhihu.com',headers = self.header)
		self._xsrf = str(BeautifulSoup(r.text).find_all('input',attrs = {"name":"_xsrf"})).split('"')[5]
		form_data = {
		'_xsrf':self._xsrf,
		'email':'',
		'password':'',
		'rememberme':'y'
		}

		self.http.post('http://www.zhihu.com/login',data = form_data, headers = self.header)

	def dbcommit(self):
		pass

	def getentryid(self,table,field,value,createnew = True):
		return none

	def addtoindex(self,url,soup):
		try:
			# print('Indexing ',url,' ',len(pagelist))
			if(self.pattern_que.fullmatch(url)):
				que_num = url.split('question/')[1]
				return_que_ans = {}
				return_que = {}
				labels = soup.find_all('a',class_ = 'zm-item-tag')
				follow = soup.find('div',class_ = 'zh-question-followers-sidebar').find('strong').text
				head = soup.title.text
				anss = soup.find_all('div',class_ = 'zm-item-answer')
				detail_node = soup.find(id = 'zh-question-detail')
				if(detail_node == None):
					detail = ''
				else:
					detail = detail_node.text[1:-1]
				for ans in anss :
					ans_good = ans.find('span',class_ = 'count').text
					tem = ans.find(class_ = 'zm-item-link-avatar')
					if(tem == None):
						ans_peo = "nominate"
					else :
						ans_peo = tem['href'].split('/people/')[1]
					ans_text = ans.find('div',class_ = 'zm-item-rich-text').text
					ans_id = ans['data-aid']
					if(ans_good[-1]=="K"):
						ans_good_int = int(ans_good[:-1])*1000
					elif(ans_good[-1]=="W"):
						ans_good_int = int(ans_good[:-1])*10000
					else:
						ans_good_int = int(ans_good)
					if(len(ans_text) > 10000):
						ans_text = ans_text[1:10000]
					else:
						ans_text = ans_text[1:-1]
					self.dbcon.db_answer(ans_id,ans_good_int,ans_peo,ans_text,que_num)
				self.dbcon.db_question(que_num,follow,head,detail)
				for label in labels:
					self.dbcon.db_label(label.text[1:-1],que_num)
			elif(self.pattern_peo.fullmatch(url)):
				peo_id = url.split('people/')[1]
				sidebar = soup.find(class_ = 'zm-profile-side-following zg-clear').find_all('strong')
				followees = int(sidebar[0].text)
				followers = int(sidebar[1].text)
				agree = int(soup.find(class_ = 'zm-profile-header-user-agree').find('strong').text)
				thanks = int(soup.find(class_ = 'zm-profile-header-user-thanks').find('strong').text)
				asks = int(soup.find(href = '/people/' + peo_id + '/asks').find('span').text)
				answers = int(soup.find(href = '/people/' + peo_id + '/answers').find('span').text)
				topics = int(soup.find(href = '/people/'+peo_id+'/topics').find('strong').text[:-4])
				followees_list =''
				followers_list=''
				if(followees > 0):
					peo_get = self.http.get(url+'/followees')
				elif(followers > 0):
					peo_get = self.http.get(url+'/followers')
				if(followees > 0 or followers > 0):
					peo_soup = BeautifulSoup(peo_get.text)
					hash_id = json.loads(peo_soup.find(class_ = 'zh-general-list clearfix')['data-init'])['params']['hash_id']
					followees_count = 0
					followers_count = 0
					while (followees_count < followees and followees_count < 50):
						# print("Getting Followees offset {0}".format(followees_count))
						params = '{' + '"offset":{0},"order_by":"created","hash_id":"{1}"'.format(followees_count,hash_id) + '}'
						form_data = {
							'method':'next',
							'params':params,
							'_xsrf':self._xsrf
						}
						followee_get = self.http.get('http://www.zhihu.com/node/ProfileFolloweesListV2',data = form_data,headers = self.header)
						followees_list = followees_list + followee_get.text
						followees_count = followees_count + 20
					while (followers_count < followers and followers_count < 50):
						# print("Getting Followers offset {0}".format(followers_count))
						params = '{' + '"offset":{0},"order_by":"created","hash_id":"{1}"'.format(followers_count,hash_id) + '}'
						form_data = {
							'method':'next',
							'params':params,
							'_xsrf':self._xsrf
						}
						follower_get = self.http.get('http://www.zhihu.com/node/ProfileFollowersListV2',data = form_data,headers = self.header)
						followers_list = followers_list + follower_get.text
						followers_count = followers_count + 20
					followees_list = BeautifulSoup(followees_list).find_all(class_ = 'zm-list-content-title')
					followers_list = BeautifulSoup(followers_list).find_all(class_ = 'zm-list-content-title')
					for followee in followees_list:
						# print(followee.find('a')['href'].split('people/')[1])
						self.dbcon.db_people_follow(peo_id,followee.find('a')['href'].split('people/')[1])
					for follower in followers_list:
						url = follower.find('a')['href']
						if(self.isindexed(url) == False):
							pagelist.append(url)
				# return_peo = {'id':peo_id,'followees':followees,'followers':followers,'topics':topics}
				self.dbcon.db_people(peo_id,asks,thanks,agree,answers,followees,followers,topics)
			self.dbcon.db_commit();
		except:
			print(url,len(pagelist))
			print(Exception)
	def gettextonly(self,soup):
		return None

	def separatewords(self,text):
		return None

	def isindexed(self,url):
		global bfilter
		if(self.pattern_in.match(url) == None or self.pattern_forbi.fullmatch(url)) : return True
		tem = bfilter.isContain(url)
		if(tem == False):
			bfilter.addValue(url)
		return tem

	def addlinkref(self,urlForm,urlTo,linkText):
		pass

	def crawl(self):
		global pagelist
		while(len(pagelist) > 0):
			# print(len(pagelist))
			# list_lock.acquire()
			page = pagelist.popleft()
			# list_lock.release()
			try:
				c = self.http.get(page)
				if(c.status_code != 200):
					raise NameError('404 Error');
			except:
				print('could not open ',page)
				print(Exception)
				pagelist.append(page)
				continue
			soup = BeautifulSoup(c.text)
			self.addtoindex(page,soup)
			links = soup.find_all('a')
			for link in links :
				if('href' in dict(link.attrs)):
					url = urljoin(page,link['href'])
					if url.find("'") != -1 : continue
					url = url.split('#')[0]
					if url[0:4] == 'http' and not self.isindexed(url):
						if(self.pattern_answer.fullmatch(url)):
							url_que = url.split('/answer')[0]
							if(not self.isindexed(url_que)):
								pagelist.append(url_que)
						else:
							pagelist.append(url)
					linkText = self.gettextonly(link)
					self.addlinkref(page,url,linkText)
			self.dbcommit()
				

	def createindextables(self):
		pass

class timer(threading.Thread):
	def __init__(self,crawler):
		threading.Thread.__init__(self)
		self.crawler = crawler
		self.thread_stop = False
	def run(self):
		self.crawler.login()
		self.crawler.crawl()

	def stop(self):
		self.thread_stop = True

class bloomFilter:
	def __init__(self,seeds,lengh):
		self.seeds = seeds
		self.lengh = lengh
		self.bits = BitVector.BitVector(size = lengh)
	def addValue(self,string):
		for seed in self.seeds :
			self.bits[self.hash(string,seed)] = 1

	def hash(self,string,seed):
		result = 0
		for i in range(len(string)):
			result = seed * result + ord(string[i])
		return result%self.lengh

	def isContain(self,string):
		for seed in self.seeds:
			if(self.bits[self.hash(string,seed)] == 0):
				return False
		return True


crawl_list = ['http://www.zhihu.com','http://www.baidu.com']
global pagelist,bfilter
pagelist = deque(crawl_list)
bfilter = bloomFilter([1,3,5,11,13,17,19,23],8000000)

for i in range(1):
	timer(crawler(dbController('root','saber','zhihu'))).start()
	time.sleep(1)
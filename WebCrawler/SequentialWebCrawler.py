from collections import deque
from bs4 import BeautifulSoup, NavigableString
import urllib.request
import urllib.robotparser
import urllib.parse
import validators
import pathlib
import re

def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 2, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()


class SequentialWebCrawler:
    def __init__(self, urls, agent, limit):
        self.inputUrls = urls
        self.userAgent = agent
        self.visitLimit = limit

    def crawl(self):
        counterVisited = 0
        counterFound = 0
        queue = deque()
        graph = {}

        for url in self.inputUrls:
            queue.append(url)

        printProgressBar(0, self.visitLimit, prefix = 'Progress:', suffix = 'Complete', length = 50)
        
        while len(queue) > 0 and counterVisited < self.visitLimit:
            elem = queue.popleft()
           
            if graph.get(elem) != None:
                continue

            counterVisited += 1
            s = '[Visited = ' + format(counterVisited) + ' Urls found = ' + format(counterFound) + ']'
            printProgressBar(counterVisited , self.visitLimit, prefix = 'Progress:', suffix = s, length = 50)

            #print(bcolors.ORANGE + "--> Processing" + bcolors.ENDC, elem, " Visited: " + format(counterVisited), " Found: " + format(counterFound))

            graph[elem] = []

            try:
                if self.allow(elem) == False:
                    continue

                htmlPage = urllib.request.urlopen(elem)
                soup = BeautifulSoup(htmlPage, 'html.parser')

                self.saveHtmlText(soup,elem)

                links = soup.find_all('a', href=True)
                for link in links:
                    url = self.getUrl(link.get('href'))
                    if url != None:
                        queue.append(url)
                        graph[elem].append(url)
                        counterFound += 1
                        
            except Exception as e:
                print("[EXCEPTION]", e)

        return graph

    def saveHtmlText(self, soup, url):
        if url.startswith('https'):
            url = re.sub(r'https://', '', url)
        if url.startswith('http'):
            url = re.sub(r'http://', '', url)
        if url.startswith('www.'):
            url = re.sub(r'www.', '', url)

        path = "Data/Pages/"+url
        pathlib.Path(path).mkdir(parents=True, exist_ok=True)
        
        text = soup.find_all(text=True)

        output = ''
        blacklist = [
            '[document]',
            'noscript',
            'header',
            'html',
            'meta',
            'head', 
            'input',
            'script',
            'css',
            'style',
            'link',
            # there may be more elements you don't want, such as "style", etc.
        ]

        whiteSpaces = [
            ' ',
            '\n',
            '',
        ]

        for t in text:
            if t.parent.name not in blacklist and t not in whiteSpaces:
                output += format(t).strip()
    
        f = open(path+"/html.txt", "w+", encoding='utf-8')
        f.write(output)
        f.close()

    def getUrl(self, url):
        parsed = urllib.parse.urlparse(url)

        url = parsed.scheme + '://' + parsed.netloc + \
            (parsed.path if parsed.path != '' else '/')

        if validators.url(url) == True:
            return url
        return None

    def allow(self, elem):
        robotParser = urllib.robotparser.RobotFileParser()
        robotParser.set_url(urllib.request.urljoin(elem, 'robots.txt'))
        robotParser.read()
        return robotParser.can_fetch(self.userAgent, elem) 

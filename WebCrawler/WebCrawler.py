from SequentialWebCrawler import SequentialWebCrawler
import base64
import os, shutil
import json
import pathlib
import sys

class bcolors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    ORANGE = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

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
        

urls = [
    'https://www.olx.ro/imobiliare/apartamente-garsoniere-de-vanzare/',
    'https://www.autovit.ro/',
    'https://www.emag.ro/',
    'https://www.w3schools.com/',
    'https://www.codecademy.com/',
    'https://altex.ro/home/',
]

def clearFolder(path):
    folder = path
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

def startWebCrawler(toVisit):
    crawler = SequentialWebCrawler(urls, 'researchBot', toVisit)
    graph = crawler.crawl()
    return graph

def writeToJSONFile(path, fileName, data):
    filePathNameWExt = path + fileName + '.json'
    with open(filePathNameWExt, 'w') as json_file:
        json.dump(data, json_file)

def savePages(path, data):
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)
    
    l = len(data)
    printProgressBar(0, l, prefix = 'Progress:', suffix = 'Complete', length = 50)

    index = 0
    for parentUrl in data:
        encryptedParentUrl = base64.b32encode(str.encode(parentUrl))
        stringParentUrl = format(encryptedParentUrl.decode('utf8'))    

        index += 1
        printProgressBar(index, l, prefix = 'Progress:', suffix = 'Complete', length = 50)

        for i in range(len(data[parentUrl])):
            url = data[parentUrl][i]
        
            encryptedUrl = base64.b32encode(str.encode(url))
            stringUrl = format(encryptedUrl.decode('utf8'))

            f = open(path + stringParentUrl + '_' + stringUrl, "w+")
            f.close() 

def main(toVisit):
    path = "Data/"

    print(bcolors.ORANGE + "\n---==========Cleaning Data folder==========---" + bcolors.ENDC)
    clearFolder(path)
    print("100% Complete")

    print(bcolors.ORANGE + "\n---==========Processing urls==========---" + bcolors.ENDC)
    graph = startWebCrawler(toVisit)

    print(bcolors.ORANGE + "\n---==========Saving found urls to JSON file==========---" + bcolors.ENDC)
    writeToJSONFile(path, 'source-destinations', graph)
    print("100% Complete")
    
if __name__ == "__main__":
    toVisit = sys.argv[1]
    print("To visit = ", toVisit)
    main(int(toVisit))
import base64
import json
import pathlib
import os, shutil
import glob
from collections import defaultdict 

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

def mapPhase(path, data):
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)
    
    dim = len(data)
    printProgressBar(0, dim, prefix = 'Progress:', suffix = 'Complete', length = 50)

    index = 0
    for parentUrl in data:
        index += 1
        printProgressBar(index , dim, prefix = 'Progress:', suffix = 'Complete', length = 50)

        stringParentUrl = encryptUrl(parentUrl)

        pathlib.Path(path).mkdir(parents=True, exist_ok=True)
        for i in range(len(data[parentUrl])):
            url = data[parentUrl][i]
        
            stringUrl = encryptUrl(url)

            filePath = path + format(stringUrl) + "_" + format(stringParentUrl)
            if len(filePath) < 257:
                # cmd = f"cmd /c \"echo .> {filePath}\""
                # os.system(cmd)

                f = open(filePath, "w+")
                f.close()

def reducePhase(path):
    encryptedFiles = os.listdir(path)

    dictionary = defaultdict(list) 

    dim = len(encryptedFiles)
    printProgressBar(0, dim, prefix = 'Progress:', suffix = 'Complete', length = 50)

    index = 0
    for encryptedFile in encryptedFiles:
        index += 1
        fileName = encryptedFile.split("_")

        dest = decrypt(fileName[0].encode('utf-8'))
        source = decrypt(fileName[1].encode('utf-8'))

        destDecoded = dest.decode('utf-8')
        sourceDecoded = source.decode('utf-8')

        dictionary[destDecoded].append(sourceDecoded)

        printProgressBar(index, dim, prefix = 'Progress:', suffix = 'Complete', length = 50)
    
    return dictionary

def clearFolder(path):
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)
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

def readFromJSONFile(path, fileName):
    filePathNameWExt = path + fileName + '.json'
    with open(filePathNameWExt) as json_file:
        data = json.load(json_file)
    return data
                         
def writeToJSONFile(path, fileName, data):
    filePathNameWExt = path + fileName + '.json'
    with open(filePathNameWExt, 'w') as json_file:
        json.dump(data, json_file)

def encryptUrl(url):
    encryptedParentUrl = base64.b16encode(str.encode(url))
    encrypted = format(encryptedParentUrl.decode('utf-8'))
    return encrypted

def decrypt(encrypted):
    decrypted = base64.b16decode(encrypted)
    return decrypted

def main():
    jsonPath = "Data/"
    urlsPath = "Data/Urls/"

    print("\n---==========Cleaning Urls folder==========---" )
    clearFolder(urlsPath)
    print("100% Complete")

    print("\n---========== Reading JSON file ==========---")
    data = readFromJSONFile(jsonPath, "source-destinations")
    print("100% Complete")

    print("\n---========== Mapping on disk ==========---")
    mapPhase(urlsPath, data)

    print("\n---========== Reduce phase ==========---")
    dictionary = reducePhase(urlsPath)

    print("\n---==========Saving to JSON file==========---")
    writeToJSONFile(jsonPath, 'destination-sources', dictionary)
    print("100% Complete")

if __name__ == "__main__":
   main()
 
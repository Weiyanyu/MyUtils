import json
import hashlib
import os

jsonFileName = "lib_md5.json"

# 遍历文件夹
def generateMd5(file):
    md5Map = {}
    for root, dirs, files in os.walk(file):

        # root 表示当前正在访问的文件夹路径
        # dirs 表示该文件夹下的子目录名list
        # files 表示该文件夹下的文件list
        
        # 遍历文件
        for filename in files:
            filepath = os.path.join(root,filename)
            fileExt = filepath.rsplit('.',maxsplit=1)
            
            if len(fileExt) > 1 and (fileExt[1] == 'json' or fileExt[1] == 'py'):
                continue

            with open(filepath, 'rb') as f:
                md5 = hashlib.md5(f.read()).hexdigest()
                md5Map[filename] = md5
            
        outJson = json.dumps(md5Map, indent=4)

        outFile = open(jsonFileName, 'w')
        outFile.write(outJson)
        outFile.close()

def watchAndChangeMD5(dirPath, md5file):
    md5Map = {}
    with open(md5file) as f:
        md5Map = json.load(f)

    for root, dirs, files in os.walk(dirPath):
        for filename in files:
            filepath = os.path.join(root,filename)
            fileExt = filepath.rsplit('.',maxsplit=1)
            
            if len(fileExt) > 1 and (fileExt[1] == 'json' or fileExt[1] == 'py'):
                continue

            with open(filepath, 'rb') as f:
                oldMd5 = md5Map[filename]
                newMd5 = hashlib.md5(f.read()).hexdigest()
                if oldMd5 != newMd5:
                    print("new content!!! : ", filename)
                    md5Map[filename] = newMd5

        outJson = json.dumps(md5Map, indent=4)

        outFile = open(jsonFileName, 'w')
        outFile.write(outJson)
        outFile.close()


            

if __name__ == "__main__":
    if os.path.exists('/home/weiyanyu/test/' + jsonFileName) == False:
        generateMd5("/home/weiyanyu/test")
    watchAndChangeMD5("/home/weiyanyu/test/", "/home/weiyanyu/test/" + jsonFileName)


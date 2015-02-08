'''
Created on 2014. 12. 4.

@author: Seokyong Hong
'''
import os 
import time

class FileWriter(object):
    BASE_DIRECTORY = '.'
    
    def __init__(self, name):
        self.name = name
    
    def open(self):
        try:
            self.path = os.path.join(FileWriter.BASE_DIRECTORY, 'result', self.name)
           
            if not os.path.exists(self.path):
                os.makedirs(self.path)
            
            filename = str(time.time()) + '.txt'
            self.file = open(os.path.join(self.path, filename), 'w')
        except:
            print 'Cannot open ' + self.file + '.'
            self.file = None
        
    def writeLine(self, line):
        if self.file is not None:
            self.file.write(line + '\n')
    
    def close(self):
        if self.file is not None:
            self.file.close()    
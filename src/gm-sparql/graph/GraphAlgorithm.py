'''
Created on 2014. 11. 5.

@author: Seokyong Hong
'''
from abc import ABCMeta, abstractmethod

class GraphAlgorithm(object):
    __metaclass__ = ABCMeta
    
    def __init__(self, name):
        self.name = name
    
    def __str__(self):
        return 'Graph Algorithm: {0}.'.format(self.name)
    
    @abstractmethod
    def initialize(self):
        raise RuntimeError('The initialize function must be overriden.')
    
    @abstractmethod
    def process(self):
        raise RuntimeError('The process function must be overriden.')
    
    @abstractmethod
    def finalize(self):
        raise RuntimeError('The finalize function must be overriden.')
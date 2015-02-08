# Copyright 2013 YarcData LLC, a Cray Company. All Rights Reserved.

import time
import signal
import traceback
import logging
import multiprocessing
from multiprocessing.pool import Pool, Process
from datetime import datetime

log = None

class LogExtra:

    def __init__( self ):
        self.start = time.time()

    def __getitem__( self, name ):
        """
        To allow this instance to look like a dict.
        """
        if name == "timehms":
            return datetime.now().strftime("%H:%M:%S")
        elif name == "timerelsec":
            return int( round( ( time.time() - self.start ) ) )
        else:
            result = self.__dict__.get(name, "?")
        return result

    def __iter__( self ):
        """
        To allow iteration over keys, which will be merged into
        the LogRecord dict before formatting and output.
        """
        keys = [ "timehms", "timerelsec" ]
        keys.extend( self.__dict__.keys() )
        return keys.__iter__()

#
# http://docs.python.org/2.6/library/multiprocessing.html#module-multiprocessing.pool
#

#
# http://stackoverflow.com/questions/6728236/exception-thrown-in-multiprocessing-pool-not-detected
#
class LogExceptions( object ):
    def __init__( self, callable ):
        self.__callable = callable
        return

    def __call__(self, *args, **kwargs):
        try:
            result = self.__callable(*args, **kwargs)

        except Exception as e:
            # Here we add some debugging help. If multiprocessing's
            # debugging is on, it will arrange to log the traceback
            log.error( traceback.format_exc() )
            # Re-raise the original exception so the Pool worker can
            # clean up
            raise

        # It was fine, give a normal answer
        return result

# http://stackoverflow.com/a/11312948
def ignoreSigInt():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

class PoolPlus( Pool ):

    def __init__( self, processes ):
        Pool.__init__( self, processes, ignoreSigInt )

    def apply_async( self, func, args = (), kwds = {}, callback = None ):
        return Pool.apply_async( self, LogExceptions( func ), args, kwds, callback )

    def go( self, func, *args, **kwargs ):
        return self.apply_async( func, args, kwargs )

def process( target, *args, **kwargs ):
    return Process( target = LogExceptions( target ), name = target.__name__, args = args, kwargs = kwargs )

# class YProcess( Process ):
#     def __init__( self, target, *args, **kwargs ):
#         Process.__init__( self, None, LogExceptions( target ), target.__name__, args, kwargs )
#     def start( self ):
#         return Process.run( self )

# class MPQueue ( Queue ):
#     def __init__( self ):
#         MPQueue.__init__( self )

class Result:

    def __init__( self, o ):
        self.o

    def get( self ):
        return self.o

# no extra procs
class SingletonPool():
    def apply_async(self, func, args=(), kwds={}, callback=None):
        result = apply(func, args, kwds)
        if (callback):
            callback(result)
        return Result( result )

    def terminate(self):
        pass

    def close(self):
        pass

    def join(self):
        pass

class WithPool:

    @staticmethod
    def prepare_optparse( optionParser ):
        optionParser.add_option( "--procs", type="int", metavar="NUM",
                                 help="number of extra processes to handle work [%default]"
                                 + " (1 uses no extra procs, NUM<=0 will subtract from cpu_count=" + str(multiprocessing.cpu_count()) + ")" )

    def run( self, options, mainfn, exitfn ):
        """mainfn( pool )
        exitfn( pool )
        """
        pool = None
        if ( options.procs is not None ):
            if ( options.procs < 1 ):
                options.procs = multiprocessing.cpu_count() + options.procs
        try:
            if ( options.procs is None or options.procs < 2 ):
                pool = PoolPlus( processes = 1 )
                # pool = SingletonPool()
            else:
                pool = PoolPlus( processes = ( options.procs ) )
            mainfn( pool )
            if ( pool ):
                exitfn( pool )
                pool.close()
                pool.join()
        except KeyboardInterrupt:
            if ( pool ):
                pool.terminate()
                pool.join()
        except Exception:
            if ( pool ):
                pool.terminate()
                pool.join()
            raise

def hbytes( bytes ):
    if ( bytes is None ):
        return "-"
    try:
        bytes = int( bytes )
    except ValueError:
        return bytes
    if ( bytes < 2048 ):
        return "%dk" % bytes
    elif ( bytes < 2048 * 1024 ):
        return "%dM" % ( bytes / 1024 )
    elif ( bytes < 2048 * 1024 * 1024 ):
        return "%dG" % ( bytes / ( 1024 * 1024 ) )

def init_logging( debug = False, logdir = None ):
    """Logging, see http://docs.python.org/2.6/library/multiprocessing.html#logging
    and see http://docs.python.org/2.6/library/logging.html#module-logging
    """
    # if ( options.procs > 1 ):
    #     log = multiprocessing.get_logger()
    # else:
    log = logging.getLogger("Main")
    log.setLevel( logging.DEBUG )

    console = logging.StreamHandler()
    # if ( debug and False ):
    #     console.setLevel( logging.DEBUG )
    # else:
    console.setLevel( logging.INFO )

    # set a format which is simpler for console use
    console.setFormatter( logging.Formatter('[%(levelname)-1s %(timehms)s] %(message)s') )
    log.addHandler( console )

    if ( logdir is not None ):
        file = logging.FileHandler( logdir + "/log_error.log" )
        file.setLevel( logging.ERROR )
        file.setFormatter( logging.Formatter('[%(levelname)-5s %(asctime)s] %(message)s') )
        log.addHandler( file )

        file = logging.FileHandler( logdir + "/log_info.log" )
        file.setLevel( logging.INFO )
        file.setFormatter( logging.Formatter('[%(levelname)-5s %(asctime)s] %(message)s') )
        log.addHandler( file )

        file = logging.FileHandler( logdir + "/log_debug.log" )
        file.setLevel( logging.DEBUG )
        # file.setFormatter( logging.Formatter('[%(levelname)-5s %(asctime)s %(processName)-12s] %(message)s') )
        file.setFormatter( logging.Formatter('[%(levelname)-5s %(asctime)s] %(message)s') )
        log.addHandler( file )

    log = logging.LoggerAdapter( log, LogExtra() )

    if ( logdir is not None ):
        log.info( "Log dir: %s", logdir )
    return log

log = logging.LoggerAdapter( logging.getLogger( "Main" ), LogExtra() )

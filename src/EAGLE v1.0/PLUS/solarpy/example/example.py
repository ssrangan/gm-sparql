# Example to use urika.py

import logging
import traceback
import optparse

import solarpy.yarc.urika
from solarpy.yarc.Tracking import Tracking

log = logging.getLogger("Main")

class ExampleOptions ( solarpy.yarc.urika.UrikaOptions ):

    def __init__( self ):
        solarpy.yarc.urika.UrikaOptions.__init__( self )

def main():
    func = "main"

    defaults = ExampleOptions()

    parser = optparse.OptionParser()
    solarpy.yarc.urika.prepare_options( parser, defaults )

    # parse_args will automatically exit if --help is used
    (options, args) = parser.parse_args()

    print options, args


##
##    tracking = Tracking( options, log )
##    track = tracking.context( tracking.ns.example, logging.INFO )
##
##    urika = solarpy.yarc.urika.Urika( options, log, logging.DEBUG, track.context( track.ns.urika, logging.DEBUG ) )
##    urika.login()
##
##    example_query = urika.blockt( __file__, "example.rq" )
##
##    print urika.query( func, example_query, None, accept = "csv" )
##
##    # urika.checkpoint_create( func, "loaded a file." )

if __name__ == '__main__':
    main()


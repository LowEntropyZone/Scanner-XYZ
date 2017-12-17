import configargparse
from itertools import cycle
from threading import Thread
import logging
logging.basicConfig(level=logging.INFO,
    format='%(asctime)s [%(threadName)16s][%(module)14s][%(levelname)8s] %(message)s')
logging.getLogger('pgoapi').setLevel(logging.WARNING)
log = logging.getLogger(__name__)
from time import sleep
from search import *
from mrmime import init_mr_mime

def scan():
    log.info('Initializing.')
    parser = configargparse.ArgumentParser()
    parser.add_argument('-st', '--step-limit', default=6)
    parser.add_argument('-l', '--scan-location')
    parser.add_argument('-ac', '--accounts-file', default='accounts.csv')
    parser.add_argument('-pf', '--proxy-file', default='proxies.txt')
    parser.add_argument('-lpf', '--login-proxy-file', default='proxies.txt')
    parser.add_argument('-ld', '--login-delay', default=6)
    parser.add_argument('-hk', '--hash-key')
    parser.add_argument('-sd', '--scan-delay', default=10)
    parser.add_argument('-lt', '--login-timeout', default=15)
    parser.add_argument('-kph', '--kph', default=35)
    parser.add_argument('-bh', '--beehiving', action='store_true')
    parser.add_argument('-lf', '--locations-file', default='coords.txt')
    parser.add_argument('-dd', '--ditto-detection', action='store_true')
    parser.add_argument('-spin', '--spin-pokestops', action='store_true')
    parser.add_argument('-nff', '--no-full-flow', action='store_true')
    parser.add_argument('-lp', '--lure-party', action='store_true')
    parser.add_argument('-psu', '--pgscout-url', default=None)
    parser.add_argument('-enc', '--encounter', action='store_true')
    parser.add_argument('-ef', '--encounter-file', default=None)
    parser.add_argument('-ss', '--spawn-scan', action='store_true')
    #parser.add_argument('-dbn', '--db-name')
    #parser.add_argument('-dbu', '--db-user')
    #parser.add_argument('-dbp', '--db-pass')
    #parser.add_argument('-dbh', '--db-host', default='localhost')
    #parser.add_argument('-dbpr', '--db-port', default=3306)
    parser.add_argument('-wh', '--webhook', action='store_true')
    parser.add_argument('-whf', '--webhook-file', default='webhooks.txt')
    args = parser.parse_args()
    #init_database(args)
    init_mr_mime({'full_login_flow': not args.no_full_flow, 'scan_delay': int(args.scan_delay)})
    create_tables()
    proxies = read_file_content(args.proxy_file)
    login_proxies = read_file_content(args.login_proxy_file)

    if args.webhook:
        webhooks = read_file_content(args.webhook_file)
    else:
        webhooks = []

    accounts = read_file_content(args.accounts_file)

    populate_accounts_queue(accounts, cycle(proxies), cycle(login_proxies))

    if args.spawn_scan:
        scheduler = SpawnpointScheduler(args)
        scheduler.schedule()
    else:
        scheduler = Scheduler(args)
        scheduler.schedule()

    t = Thread(target=db_queue_inserter,name='db-inserter', args=(webhooks, ))
    t.start()

    ss = Thread(target=spawn_stats,name='spawn-stats',args=(scheduler, ))
    ss.start()

    if args.encounter and args.pgscout_url != None:
        try:
            list_enc = [int(i) for i in read_file_content(args.encounter_file)]
        except:
            list_enc = []
    else:
        list_enc = []

    i = 0
    while i < len(accounts):
        t = Thread(target=search_worker, name='search-worker-{}'.format(i), args=(args, scheduler, list_enc, ))
        #t.daemon = True
        t.start()
        sleep(int(args.login_delay))
        i += 1

def read_file_content(fn):
    try:
        f = open(fn)
        l = f.readlines()
        f.close()
        return l
    except Exception as e:
        log.error(repr(e))
        return []

if __name__ == '__main__':
    scan()

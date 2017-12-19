# from threading import Lock as ThreadLock
import calendar
import logging
from queue import Queue
from random import choice
from time import sleep

import geopy.distance

from transform import *

log = logging.getLogger(__name__)

from utils import *
from pgscout import *
from db import *

l30_account_queue = Queue()
account_queue = Queue()

dbq = Queue()

encounter_queue = Queue()

# literally the most important import
from mrmime.pogoaccount import *
from mrmime.shadowbans import is_rareless_scan

wh_cache = []

def calc_pokemon_level(cp_multiplier):
    if cp_multiplier < 0.734:
        level = 58.35178527 * cp_multiplier * cp_multiplier - 2.838007664 * cp_multiplier + 0.8539209906
    else:
        level = 171.0112688 * cp_multiplier - 95.20425243
    level = (round(level) * 2) / 2.0
    return int(level)

def add_item_to_wh_cache(item):
    wh_cache.append(item)

def get_wh_type(model):
    if model == Pokemon:
        return 'pokemon'
    if model == Raid:
        return 'raid'

def create_webhook_item(model, data):
    if model == Pokemon:
        return {
            "pokemon_id": data['pokemon_id'],
            "encounter_id": b64_e(data['encounter_id']),
            "latitude": data['latitude'],
            "longitude": data['longitude'],
            "last_modified_time": now_ms(),
            "spawnpoint_id": data['spawnpoint_id'],
            "disappear_time": calendar.timegm(data['disappear_time'].timetuple()),
            "pokemon_level": data.get('level'),
            "cp": data.get('cp'),
            "height": data.get('height'),
            "weight": data.get('weight'),
            "gender": data.get('gender'),
            "form": data.get('form'),
            "move_1": data.get('move_1'),
            "move_2": data.get('move_2'),
            "individual_attack": data.get('individual_attack'),
            "individual_defense": data.get('individual_defense'),
            "individual_stamina": data.get('individual_stamina'),
        }
    elif model == Raid:
        return {
            'gym_id': b64_e(data['gym_id']),
            'latitude': data['latitude'],
            'longitude': data['longitude'],
            'spawn': dt_to_ts(data['spawn']-timedelta(hours=8)),
            'start': dt_to_ts(data['start']-timedelta(hours=8)),
            'end': dt_to_ts(data['end']-timedelta(hours=8)),
            'level': data['level'],
            'pokemon_id': data.get('pokemon_id', None),
            'cp': data.get('cp', None),
            'move_1': data.get('move_1', None),
            'move_2': data.get('move_2', None)
        }

def db_queue_inserter(webhooks):
    while True:
        try:
            model, data = dbq.get()
            with database.atomic():
                try:
                    model.create(**data)
                except IntegrityError as e:
                    if 'Duplicate entry' in str(e):
                        if model == Pokemon:
                            try:
                                Pokemon.get(Pokemon.encounter_id == data['encounter_id']).delete_instance()
                                Pokemon.create(**data)
                            except Exception as e:
                                log.error(repr(e))
                        if model == Raid:
                            try:
                                Raid.get(Raid.raid_seed == data['raid_seed']).delete_instance()
                                Raid.create(**data)
                            except Exception as e:
                                log.error(repr(e))
                    else:
                        log.error(repr(e))

            whdata = create_webhook_item(model, data)
            type = get_wh_type(model)
            if type == 'raid' and whdata['pokemon_id'] == None:
                type = 'egg'
            for wh in webhooks:
                requests.post(wh, data=json.dumps({
                    'type': type,
                    'message': whdata
                }))
        except Exception as e:
            log.error(repr(e))

class BaseScheduler(object):
    def __init__(self, args, location):
        self.args = args
        self.scan_location = location

    def generate_locations(self):
        pass

    def schedule(self):
        pass

    def next_item(self, status):
        pass

    def item_done(self,i):
        pass

class Scheduler(BaseScheduler):
    def __init__(self, args, location):
        super(Scheduler, self).__init__(args, location)
        self.args = args
        self.queue = []

        self.step_distance = 0.07
        self.step_limit = int(args.step_limit)

        self.locations = []

        self.lock = Lock()

    def generate_locations(self):
        NORTH = 0
        EAST = 90
        SOUTH = 180
        WEST = 270

        # Dist between column centers.
        xdist = math.sqrt(3) * self.step_distance
        ydist = 3 * (self.step_distance / 2)  # Dist between row centers.

        results = []

        results.append((self.scan_location[0], self.scan_location[1], 0))

        if self.step_limit > 1:
            loc = self.scan_location

            # Upper part.
            ring = 1
            while ring < self.step_limit:

                loc = get_new_coords(
                    loc, xdist, WEST if ring % 2 == 1 else EAST)
                results.append((loc[0], loc[1], 0))

                for i in range(ring):
                    loc = get_new_coords(loc, ydist, NORTH)
                    loc = get_new_coords(
                        loc, xdist / 2, EAST if ring % 2 == 1 else WEST)
                    results.append((loc[0], loc[1], 0))

                for i in range(ring):
                    loc = get_new_coords(
                        loc, xdist, EAST if ring % 2 == 1 else WEST)
                    results.append((loc[0], loc[1], 0))

                for i in range(ring):
                    loc = get_new_coords(loc, ydist, SOUTH)
                    loc = get_new_coords(
                        loc, xdist / 2, EAST if ring % 2 == 1 else WEST)
                    results.append((loc[0], loc[1], 0))

                ring += 1

            # Lower part.
            ring = self.step_limit - 1

            loc = get_new_coords(loc, ydist, SOUTH)
            loc = get_new_coords(
                loc, xdist / 2, WEST if ring % 2 == 1 else EAST)
            results.append((loc[0], loc[1], 0))

            while ring > 0:

                if ring == 1:
                    loc = get_new_coords(loc, xdist, WEST)
                    results.append((loc[0], loc[1], 0))

                else:
                    for i in range(ring - 1):
                        loc = get_new_coords(loc, ydist, SOUTH)
                        loc = get_new_coords(
                            loc, xdist / 2, WEST if ring % 2 == 1 else EAST)
                        results.append((loc[0], loc[1], 0))

                    for i in range(ring):
                        loc = get_new_coords(
                            loc, xdist, WEST if ring % 2 == 1 else EAST)
                        results.append((loc[0], loc[1], 0))

                    for i in range(ring - 1):
                        loc = get_new_coords(loc, ydist, NORTH)
                        loc = get_new_coords(
                            loc, xdist / 2, WEST if ring % 2 == 1 else EAST)
                        results.append((loc[0], loc[1], 0))

                    loc = get_new_coords(
                        loc, xdist, EAST if ring % 2 == 1 else WEST)
                    results.append((loc[0], loc[1], 0))

                ring -= 1

        # This will pull the last few steps back to the front of the list,
        # so you get a "center nugget" at the beginning of the scan, instead
        # of the entire nothern area before the scan spots 70m to the south.
        if self.step_limit >= 3:
            if self.step_limit == 3:
                results = results[-2:] + results[:-2]
            else:
                results = results[-7:] + results[:-7]

        return results

    def schedule(self):
        self.locations = self.generate_locations()
        for l in self.locations:
            self.queue.append(l)
            #log.info(str(self.queue))

    def next_item(self, status):
        #log.info(str(self.queue))
        #log.info(len(self.queue))
        #self.lock.acquire()
        if len(self.queue)==0:
            self.schedule()

        best = {}
        #best_score = 1e12
        if status.get('latitude', None) == None:
            return choice(self.queue)
        else:
            for l in self.queue:
                score = 1e12
                dist = geopy.distance.vincenty((status['latitude'], status['longitude']), l).meters
                p = geopy.Point(l[0], l[1])
                score = score / (dist + 10.)
                log.debug('Score: {} (Coords: {})'.format(str(score), p.format_decimal()))
                if score > best.get('score', 0.):
                    log.debug('New best! {} with a score of {}'.format(p.format_decimal(), str(score)))
                    best.update({
                        'latitude': l[0],
                        'longitude': l[1],
                        'score': score
                    })

            #log.info(len(self.queue))
            self.queue.remove((best['latitude'], best['longitude'], 0))
            #self.lock.release()
            return (best['latitude'], best['longitude'])

    def item_done(self, i):
        pass

class SpawnpointScheduler(BaseScheduler):
    def __init__(self, args, location):
        super(SpawnpointScheduler, self).__init__(args, location)
        self.args = args
        self.queue = []

        self.step_limit = int(args.step_limit)

    def schedule(self):
        self.generate_locations()

    def generate_locations(self):
        spawns = SpawnPoint.get_spawnpoints_in_hex(center=self.scan_location, steps=self.step_limit, quiet=False)

        for s in spawns:
            spawn_proto = {
                'tth_known': s['tth_secs'] != None
            }
            spawn_proto.update(s)

            self.queue.append(spawn_proto)

    def next_item(self, status):
        if len(self.queue) == 0:
            self.schedule()

        if 'latitude' not in status or 'longitude' not in status:
            choice = random.choice(self.queue)
            self.queue.remove(choice)
            return (choice['latitude'], choice['longitude'])

        best = {'score': 0., 'raw_data': {}}

        for s in self.queue:
            score = 1e12 if not s['tth_known'] else 1
            dist = geopy.distance.vincenty((status['latitude'], status['longitude']), (s['latitude'], s['longitude'])).meters
            score = score / (dist + 10.)
            if s['tth_known']:
                time_until_tth = (s['tth_secs']+3600)-cur_secs()
                if time_until_tth > 1800:
                    time_until_tth -= 1800
                score += time_until_tth

            log.debug('Score: {}'.format(str(score)))
            if score > best.get('score', 0.):
                #log.debug('New best! {} with a score of {}'.format(p.format_decimal(), str(score)))
                best.update({
                    'score': score,
                    'raw_data': s
                })

        s = best['raw_data']
        self.queue.remove(s)
        return (s['latitude'], s['longitude'])

    def item_done(self,i):
        pass


def spawn_stats(scheduler):
    # scheduler = Scheduler()
    while True:
        spawns = SpawnPoint.get_spawnpoints_in_hex(scheduler.scan_location, scheduler.step_limit, True)
        tth = []
        unknown = []
        #print(spawns)
        for s in spawns:
            if s['tth_secs'] != None:
                tth.append(s)
            else:
                unknown.append(s)

        log.info('{} spawns known, with {} having known timers, and {} unknown timers.'.format(len(spawns), len(tth), len(unknown)))

        sleep(120)

def populate_accounts_queue(acs, proxy_cycle, login_proxy_cycle):
    for a in acs:
        d = a.split(',')
        proxy = next(proxy_cycle, None)
        login_proxy = next(login_proxy_cycle, None)
        account_queue.put({
            'provider': d[0],
            'username': d[1],
            'password': d[2].replace('\n', ''),
            'proxy': proxy,
            'login_proxy': login_proxy
        })

def create_api(args, details, loc):
    try:
        api = POGOAccount(
            auth_service=details.get('provider', 'ptc'),
            username=details.get('username'),
            password=details.get('password'),
            hash_key=args.hash_key,
            proxy_url=details.get('proxy', None)
        )
        api.set_position(loc[0], loc[1], alt=0)
        api.log_info('Setting up at {}, {}.'.format(loc[0], loc[1]))
        #api._api.get_auth_provider().set_proxy(proxy_config(details.get('login
        # _proxy', None)))
        api.latitude = loc[0]
        api.longitude = loc[1]

        if not api.check_login():
            log.error('Account {} failed to log in.'.format(api.username))

        if api.is_banned():
            log.error('Account {} is banned.'.format(api.username))

        if api.is_warned():
            log.warning('Account {} is warned.'.format(api.username))

        if api.has_captcha():
            log.error('Account {} has a CAPTCHA.'.format(api.username))

        return api
    except Exception as e:
        log.error(repr(e))

DITTO_IDS = [16, 19, 41, 161, 163, 193]

def search_worker(args, scheduler, enc_list):
    #scheduler = Scheduler(args)
    details = account_queue.get()
    initial_location = scheduler.next_item(details)
    #log.info(str(initial_location))
    details['latitude'] = initial_location[0]
    details['longitude'] = initial_location[1]
    try:
        api = create_api(args, details, initial_location)
        #api.log_info('Setting up at {}, {}.'.format(initial_location[0],
        # initial_location[1]))
        last_action = datetime.utcnow()
        #log.info('Setting last action')
        rareless_scan_count = 0
        #log.info('Setting rareless scans count')
        if not api.is_logged_in():
            log.error('Account {} not logged in.'.format(api.username))
            return

        if api.is_banned():
            log.error('Account {} banned.'.format(api.username))
            return

        if api.has_captcha():
            log.error('Account {} CAPTCHA\'d.'.format(api.username))
            return

        api.log_info('Level: {} ({} / {} XP), Pokestops spun: {}, Pokemon '
                     'caught: {}, KM Walked: {}'.format(
            api.get_stats('level', 1),
            api.get_stats('experience', 0),
            api.get_stats('next_level_xp', 0),
            api.get_stats('poke_stop_visits', 0),
            api.get_stats('pokemons_captured', 0),
            api.get_stats('km_walked', 0.)
        ))
    except Exception as e:
        log.error(repr(e))
        return

    while True:
        #log.info('Pulling location')
        loc = scheduler.next_item(details)
        #log.info('Pulled location')
        #log.info(str(loc))
        gp = geopy.Point(loc[0], loc[1])
        now_date = datetime.utcnow()
        meters = geopy.distance.vincenty(loc, (api.latitude, api.longitude))\
            .meters
        secs_to_arrival = meters / float(args.kph) * 3.6
        secs_waited = (now_date - last_action).total_seconds()
        secs_to_arrival = max(secs_to_arrival - secs_waited, 0)
        api.log_info('Moving to {} (will take {} seconds to travel)'
                     .format(gp.format_decimal(), round(secs_to_arrival, 2)))
        #log.info('{} seconds to arrive'.format(secs_to_arrival))
        sleep(secs_to_arrival)
        api.set_position(loc[0], loc[1], 0)
        details['latitude'] = loc[0]
        details['longitude'] = loc[1]
        gmo = api.req_get_map_objects()['GET_MAP_OBJECTS']

        try:
            weather_info = gmo.client_weather[0]

            # NONE (0)
            # CLEAR (1)
            # RAINY (2)
            # PARTLY_CLOUDY (3)
            # OVERCAST (4)
            # WINDY (5)
            # SNOW (6)
            # FOG (7)
            condition = weather_info.gameplay_weather.gameplay_condition
        except Exception:
            condition = 0

        cells = gmo.map_cells
        rareless = is_rareless_scan(gmo)
        if rareless:
            rareless_scan_count += 1

        if not rareless:
            rareless_scan_count = 0

        if rareless_scan_count > 10:
            log.warning('Account {} may be shadowbanned. {} scans without rare '
                        'Pokemon.'.format(api.username, rareless_scan_count))

        if rareless_scan_count >= 25:
            # Die.
            log.error('Account {} is shadowbanned. {} scans without rare Pokemon. Exiting.'.format(api.username, rareless_scan_count))
            return

        last_action = datetime.utcnow()
        counts = {
            'pokemon': 0,
            'nearby': 0,
            'raids': 0,
            'forts': 0
        }

        for cell in cells:
            counts['nearby'] += len(cell.nearby_pokemons)

            for f in cell.forts:
                counts['forts'] += 1
                if f.raid_info.raid_level != 0:
                    counts['raids'] += 1
                    raid = f.raid_info
                    if raid.raid_battle_ms > now_ms():
                        dbq.put((Raid, {
                            'raid_seed': raid.raid_seed,
                            'gym_id': f.id,
                            'spawn': datetime.fromtimestamp(raid.raid_spawn_ms/1000),
                            'start': datetime.fromtimestamp(raid.raid_battle_ms/1000),
                            'end': datetime.fromtimestamp(raid.raid_end_ms/1000),
                            'level': raid.raid_level,
                            'pokemon_id': None,
                            'cp': None,
                            'move_1': None,
                            'move_2': None,
                            'latitude': f.latitude,
                            'longitude': f.longitude
                        }))
                    else:
                        dbq.put((Raid, {
                            'raid_seed': raid.raid_seed,
                            'gym_id': f.id,
                            'spawn': datetime.fromtimestamp(raid.raid_spawn_ms / 1000),
                            'start': datetime.fromtimestamp(raid.raid_battle_ms / 1000),
                            'end': datetime.fromtimestamp(raid.raid_end_ms / 1000),
                            'level': raid.raid_level,
                            'pokemon_id': raid.raid_pokemon.pokemon_id,
                            'cp': raid.raid_pokemon.cp,
                            'move_1': raid.raid_pokemon.move_1,
                            'move_2': raid.raid_pokemon.move_2,
                            'latitude': f.latitude,
                            'longitude': f.longitude
                        }))


            for p in cell.wild_pokemons:
                counts['pokemon'] += 1
                spawn = SpawnPoint.find_spawn(p.spawn_point_id, p.latitude, p.longitude)
                disappear = calculate_disappear(p, spawn)

                if p.pokemon_data.pokemon_id in DITTO_IDS and args.ditto_detection:
                    api.log_info('Trying to check a Ditto at {}, {}.'.format(p.latitude, p.longitude))

                    enc = api.req_encounter(
                        encounter_id=p.encounter_id,
                        spawn_point_id=p.spawn_point_id,
                        latitude=p.latitude,
                        longitude=p.longitude
                    )['ENCOUNTER']

                    if enc.status == 1:
                        i = 0
                        while i < 5:
                            api.log_info('Catch attempt #{}.'.format(i+1))
                            catch = api.req_catch_pokemon(
                                encounter_id=p.encounter_id,
                                spawn_point_id=p.spawn_point_id,
                                normalized_reticle_size=1.95,
                                spin_modifier=1.,
                                ball=1
                            )['CATCH_POKEMON']

                            if catch.status == 1:
                                api.log_info('Pokemon caught. (ID: {}, CP: {}, IV: {})'.format(api.last_caught_pokemon['pokemon_id'], api.last_caught_pokemon['cp'], get_iv(api.last_caught_pokemon)))
                                if api.last_caught_pokemon['pokemon_id'] == 132:
                                    api.log_info('Ditto found at {}, {}!'.format(p.latitude, p.longitude))
                                    try:
                                        dbq.put((Pokemon, {
                                            'encounter_id': b64_e(p.encounter_id),
                                            'spawnpoint_id': p.spawn_point_id,
                                            'pokemon_id': 132,
                                            'latitude': p.latitude,
                                            'longitude': p.longitude,
                                            'disappear_time': disappear,
                                            'gender': api.last_caught_pokemon['gender'],
                                            'form': api.last_caught_pokemon['form'],
                                            'disguise': p.pokemon_data.pokemon_id,
                                            'weather': condition
                                        }))
                                    except Exception as e:
                                        log.error(repr(e))
                                    break

                                else:
                                    api.log_info('Not a Ditto.')
                                    try:
                                        dbq.put((Pokemon, {
                                            'encounter_id': b64_e(p.encounter_id),
                                            'spawnpoint_id': p.spawn_point_id,
                                            'pokemon_id': p.pokemon_data.pokemon_id,
                                            'latitude': p.latitude,
                                            'longitude': p.longitude,
                                            'disappear_time': disappear,
                                            'gender': p.pokemon_data.pokemon_display.gender,
                                            'form': p.pokemon_data.pokemon_display.form,
                                            'weather': condition
                                        }))
                                    except Exception as e:
                                        log.error(repr(e))

                                    break

                            elif catch.status == 3:
                                try:
                                    dbq.put((Pokemon, {
                                        'encounter_id': b64_e(p.encounter_id),
                                        'spawnpoint_id': p.spawn_point_id,
                                        'pokemon_id': p.pokemon_data.pokemon_id,
                                        'latitude': p.latitude,
                                        'longitude': p.longitude,
                                        'disappear_time': disappear,
                                        'gender': p.pokemon_data.pokemon_display.gender,
                                        'form': p.pokemon_data.pokemon_display.form,
                                        'weather': condition
                                    }))
                                except Exception as e:
                                    log.error(repr(e))
                                break

                            elif catch.status not in [1, 3]:
                                sleep(0.1)
                                i += 1
                                if i > 5:
                                    break

                if p.pokemon_data.pokemon_id in enc_list:
                    scout = pgscout_encounter(p, args)
                    try:
                        dbq.put((Pokemon, {
                            'encounter_id': b64_e(p.encounter_id),
                            'spawnpoint_id': p.spawn_point_id,
                            'pokemon_id': p.pokemon_data.pokemon_id,
                            'latitude': p.latitude,
                            'longitude': p.longitude,
                            'disappear_time': disappear,
                            'gender': p.pokemon_data.pokemon_display.gender,
                            'form': p.pokemon_data.pokemon_display.form,
                            'iv_attack': scout.get('iv_attack', None),
                            'iv_defense': scout.get('iv_defense', None),
                            'iv_stamina': scout.get('iv_stamina', None),
                            'move_1': scout.get('move_1', None),
                            'move_2': scout.get('move_2', None),
                            'cp': scout.get('cp', None),
                            'cp_multiplier': scout.get('cp_multiplier', None),
                            'level': scout.get('level', None),
                            'height': scout.get('height', None),
                            'weight': scout.get('weight', None),
                            'weather': condition
                        }))
                    except Exception as e:
                        log.error(repr(e))
                else:
                    try:
                        dbq.put((Pokemon, {
                            'encounter_id': b64_e(p.encounter_id),
                            'spawnpoint_id': p.spawn_point_id,
                            'pokemon_id': p.pokemon_data.pokemon_id,
                            'latitude': p.latitude,
                            'longitude': p.longitude,
                            'disappear_time': disappear,
                            'gender': p.pokemon_data.pokemon_display.gender,
                            'form': p.pokemon_data.pokemon_display.form,
                            'weather': condition
                        }))
                    except Exception as e:
                        log.error(repr(e))

            for f in cell.forts:
                dist = geopy.distance.vincenty((api.latitude, api.longitude), (f.latitude, f.longitude)).kilometers
                if dist <= 0.04 and f.type == 1 and f.enabled and args.spin_pokestops:
                    api.log_info('Spinning a Pokestop at {}, {}.'.format(f.latitude, f.longitude))
                    api.seq_spin_pokestop(f.id, f.latitude, f.longitude, api.latitude, api.longitude)

                # DON'T LET NIANTIC THROTTLE. *SWEATING*
                sleep(random.random())

        api.log_info('Parse at {} returned {} Pokemon ({} nearby), {} forts and {} raids.'.format(gp.format_decimal(), counts['pokemon'], counts['nearby'], counts['forts'], counts['raids']))
        # Other shit.
        # details['inventory_ts'] = api._item_templates_time
        # details['ds_hash'] = api._download_settings_hash
        scheduler.item_done(loc)
        # sleep(int(args.scan_delay))

def l30_encounter_worker(args):
    details = l30_account_queue.get()

    api = create_api(args, details, [float(i) for i in args.scan_location.split(',')])

    while True:
        try:
            enc_info = encounter_queue.get()
            api.set_position(enc_info['latitude'], enc_info['longitude'], 0)
            enc = api.req_encounter(
                encounter_id=enc_info['encounter_id'],
                spawn_point_id=enc_info['spawnpoint_id'],
                latitude=enc_info['latitude'],
                longitude=enc_info['longitude']
            )['ENCOUNTER']

            if enc.status == 1:
                pass


        except Exception as e:
            log.error(repr(e))

def calculate_disappear(p, spawn):
    if 0 < p.time_till_hidden_ms < 3600000:
        d_t_secs = date_secs(datetime.utcfromtimestamp(
            (p.last_modified_timestamp_ms +
             p.time_till_hidden_ms) / 1000.0))
        SpawnPoint.update_tth(spawn.spawnpoint_id, d_t_secs)

        return datetime.utcfromtimestamp(
            (p.last_modified_timestamp_ms +
             p.time_till_hidden_ms) / 1000.0)
    else:
        if spawn.tth_secs != None:
            return start_of_hr() + timedelta(seconds=spawn.tth_secs)
        else:
            return datetime.utcnow() + timedelta(minutes=1)


def hex_bounds(center, steps=None, radius=None):
    # Make a box that is (70m * step_limit * 2) + 70m away from the
    # center point.  Rationale is that you need to travel.
    sp_dist = 0.07 * (2 * steps + 1) if steps else radius
    n = get_new_coords(center, sp_dist, 0)[0]
    e = get_new_coords(center, sp_dist, 90)[1]
    s = get_new_coords(center, sp_dist, 180)[0]
    w = get_new_coords(center, sp_dist, 270)[1]
    return (n, e, s, w)


def get_spawnpoints_in_hex(center, steps):

        log.info('Finding spawnpoints {} steps away.'.format(steps))

        n, e, s, w = hex_bounds(center, steps)

        query = (SpawnPoint
                 .select(SpawnPoint.latitude,
                         SpawnPoint.longitude,
                         SpawnPoint.spawnpoint_id,
                         SpawnPoint.tth_secs,
                         SpawnPoint.spawn_duration
                         ))
        query = (query.where((SpawnPoint.latitude <= n) &
                             (SpawnPoint.latitude >= s) &
                             (SpawnPoint.longitude >= w) &
                             (SpawnPoint.longitude <= e)
                             ))
        # Sqlite doesn't support distinct on columns.
        query = query.distinct(SpawnPoint.spawnpoint_id)

        with database.execution_context():
            s = list(query.dicts())

        # The distance between scan circles of radius 70 in a hex is 121.2436
        # steps - 1 to account for the center circle then add 70 for the edge.
        step_distance = ((steps - 1) * 121.2436) + 70
        # Compare spawnpoint list to a circle with radius steps * 120.
        # Uses the direct geopy distance between the center and the spawnpoint.
        filtered = []

        for idx, sp in enumerate(s):
            if geopy.distance.distance(
                    center, (sp['latitude'], sp['longitude'])).meters <= step_distance:
                filtered.append(s[idx])

        return filtered

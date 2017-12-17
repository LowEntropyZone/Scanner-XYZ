from peewee import *
from transform import get_new_coords
from datetime import datetime

import sys

import geopy
import geopy.distance

import logging
log = logging.getLogger(__name__)

from playhouse.pool import PooledMySQLDatabase
from playhouse.shortcuts import RetryOperationalError

class MyRetryDB(RetryOperationalError, PooledMySQLDatabase):
    pass

database = MySQLDatabase('xyzdb', **{'user': 'root'})

class UnknownField(object):
    pass

class BaseModel(Model):
    class Meta:
        database = database

class GymData(BaseModel):
    gym_id = CharField(primary_key=True, max_length=127)
    latitude = DoubleField()
    longitude = DoubleField()
    last_modified = DateTimeField(null=True)
    last_scanned = DateTimeField(default=datetime.utcnow(), null=True)
    name = TextField(null=True)
    description = TextField(null=True)
    team = IntegerField(null=True)
    defender_count = IntegerField(null=True)
    total_cp = IntegerField(null=True)

class Pokemon(BaseModel):
    encounter_id = CharField(primary_key=True, max_length=127)
    spawnpoint_id = CharField(max_length=16)
    pokemon_id = IntegerField()
    latitude = DoubleField()
    longitude = DoubleField()
    disappear_time = DateTimeField()
    gender = IntegerField(null=True)
    form = IntegerField(null=True)
    disguise = IntegerField(null=True)
    # Weather (NEW GEN3)
    weather = IntegerField(null=True)

    # IV - CP Info
    iv_attack = SmallIntegerField(null=True)
    iv_defense = SmallIntegerField(null=True)
    iv_stamina = SmallIntegerField(null=True)
    move_1 = SmallIntegerField(null=True)
    move_2 = SmallIntegerField(null=True)
    cp = IntegerField(null=True)
    cp_multiplier = FloatField(null=True)
    level = IntegerField(null=True)
    height = FloatField(null=True)
    weight = FloatField(null=True)

class Raid(BaseModel):
    raid_seed = CharField(primary_key=True, max_length=127)
    gym_id = CharField(max_length=127)
    spawn = DateTimeField()
    start = DateTimeField()
    end = DateTimeField()
    level = IntegerField()
    pokemon_id = IntegerField(null=True)
    cp = IntegerField(null=True)
    move_1 = IntegerField(null=True)
    move_2 = IntegerField(null=True)
    latitude = DoubleField()
    longitude = DoubleField()

class SpawnPoint(BaseModel):
    spawnpoint_id = CharField(primary_key=True, max_length=16)
    latitude = DoubleField()
    longitude = DoubleField()
    tth_secs = IntegerField(null=True)
    spawn_duration = IntegerField(default=1800, null=True)

    @staticmethod
    def find_spawn(id, lat, lng):
        try:
            return SpawnPoint.select().where(SpawnPoint.spawnpoint_id == id).get()
        except SpawnPoint.DoesNotExist:
            return SpawnPoint.create(spawnpoint_id=id,latitude=lat,longitude=lng)

    @staticmethod
    def update_tth(id, tth_secs, duration=1800):
        sp = SpawnPoint.select().where(SpawnPoint.spawnpoint_id == id).get()
        sp.tth_secs = tth_secs
        sp.spawn_duration = duration
        log.info('TTH found for spawn: {}'.format(id))
        sp.save()

    @staticmethod
    def get_spawnpoints_in_hex(center, steps, quiet=False):

        if not quiet:
            log.info('Finding spawnpoints {} steps away.'.format(steps))

        n, e, s, w = hex_bounds(center, steps)
        query = (SpawnPoint
                 .select(SpawnPoint.latitude,
                         SpawnPoint.longitude,
                         SpawnPoint.spawnpoint_id,
                         SpawnPoint.tth_secs,
                         SpawnPoint.spawn_duration,
                         ))
        query = (query.where((SpawnPoint.latitude <= n) &
                             (SpawnPoint.latitude >= s) &
                             (SpawnPoint.longitude >= w) &
                             (SpawnPoint.longitude <= e)
                             ))

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

def create_tables():
    with database.transaction():
        try:
            database.create_tables([GymData, Pokemon, Raid, SpawnPoint], safe=True)
        except Exception as e:
            log.error('Exception on creating tables: {}'.format(repr(e)))
            database.rollback()
            sys.exit(1)


def hex_bounds(center, steps=None, radius=None):
    # Make a box that is (70m * step_limit * 2) + 70m away from the
    # center point.  Rationale is that you need to travel.
    sp_dist = 0.07 * (2 * steps + 1) if steps else radius
    n = get_new_coords(center, sp_dist, 0)[0]
    e = get_new_coords(center, sp_dist, 90)[1]
    s = get_new_coords(center, sp_dist, 180)[0]
    w = get_new_coords(center, sp_dist, 270)[1]
    return (n, e, s, w)
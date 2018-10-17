#!/usr/bin/env python

# Simple script to create zfs snapshots on kafka node. Stop kafka, zookeper, create a snapshot, start.
# Would be nice to have some logging.
# v0.01
#
# Author: mbienek

import datetime
import subprocess
import argparse

from subprocess import Popen, PIPE, STDOUT
try:
    from subprocess import DEVNULL  # py3.x
except ImportError:
    import os
    DEVNULL = open(os.devnull, 'wb')


def service(apps=['kafka', 'zookeeper'], arg="status"):
    for app in apps:
        if arg == "stop":
            print "Stopping service: %s" % app
            rs = subprocess.check_call(["/etc/init.d/%s" % app, "stop"], stdout=DEVNULL, stderr=DEVNULL)
            print "Stopped service: %s" % app
        elif arg == "start":
            print "Starting service: %s" % app
            rs = subprocess.check_call(["/etc/init.d/%s" % app, "start"], stdout=DEVNULL, stderr=DEVNULL)
            print "Started service: %s" % app
        elif arg == "status":
            rs = subprocess.check_call(["/etc/init.d/%s" % app, "status"], stdout=DEVNULL)
            print "Service: %s is running." % app


def create_snapshot(zpoolName="tank"):
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M")

    try:
        zfsList = subprocess.check_output(["zfs", "list", "-H", "-o", "name"])
    except subprocess.CalledProcessError as err:
        print "ERROR: %s" % err
    except OSError as err:
        print "ERROR: %s. Is zfs installed?" % err

    for zfsPool in zfsList.split("\n"):
        if "%s/" % zpoolName in zfsPool:
            try:
                subprocess.check_output(["zfs", "snapshot", "%s@%s" % (zfsPool, timestamp)], stderr=DEVNULL)
            except subprocess.CalledProcessError as err:
                print "ERROR: %s" % err
            except OSError as err:
                print "ERROR: %s. Is zfs installed?" % err

            print "Created snapshot: %s@%s" % (zfsPool, timestamp)


def cleanup_snapshots(zpoolName="tank", hours=48):
    """
    Clean up snapshots older than n in a given pool.
    TODO: Doesn't check if pool exists, if there are any snapshots at all etc.
    """
    now = datetime.datetime.now()
    try:
        zfsList = subprocess.check_output(["zfs", "list", "-H", "-t", "snapshot", "-o", "name"])
    except subprocess.CalledProcessError as err:
        print "ERROR: %s" % err
    except OSError as err:
        print "ERROR: %s. Is zfs installed?" % err

    for zfsSnapshot in zfsList.split("\n"):
        if "%s/" % zpoolName in zfsSnapshot:
            dateSnapshot = datetime.datetime.strptime(zfsSnapshot.split("@")[1], "%Y%m%d-%H%M")
            if (now - dateSnapshot) >= datetime.timedelta(hours=int(hours)):
                try:
                    subprocess.check_output(["zfs", "destroy", "%s" % (zfsSnapshot)])
                except subprocess.CalledProcessError as err:
                    print "ERROR: %s" % err
                except OSError as err:
                    print "ERROR: %s. Is zfs installed?" % err

                print "Removed snapshot: %s" % (zfsSnapshot)


def monitoring(action='disable', expire='5minutes'):
    if action == 'disable':
        rs = subprocess.check_call(["sensu_silence", "--action", "silence", "-e", "%s" % expire, "-r", "Kafka backup."], stdout=DEVNULL, stderr=DEVNULL)
        print "Downtimed sensu for %s." % expire
    elif action == 'enable':
        rs = subprocess.check_call(["sensu_silence", "--action", "delete_silence"], stdout=DEVNULL, stderr=DEVNULL)
        print "Unsilenced."
    else:
        print "%s ? I don't know what to do." % action


def main():
    startTime = datetime.datetime.now()

    p = argparse.ArgumentParser(description='Super simple ZFS snapshots for kafka.')

    sub = p.add_subparsers(help='commands', dest='mode')

    snapshot = sub.add_parser('snapshot', help='Create snapshot.')
    snapshot.add_argument('-p', '--pool', help='ZFS pool to snapshot.', default='tank')
    snapshot.add_argument('-e', '--expire', help='Expire time for downtime', default='5minutes')

    clean = sub.add_parser('clean', help='Remove snapshots older than n hours.')
    clean.add_argument('-t', '--time', help='Remove snapshots older than.', default=48)
    clean.add_argument('-p', '--pool', help='ZFS pool to snapshot.', default='tank')

    args = p.parse_args()

    if args.mode == 'snapshot':
        print "==== Starting at %s ====" % startTime
        monitoring('disable', expire=args.expire)
        service(['kafka', 'zookeeper'], 'stop')
        create_snapshot(zpoolName=args.pool, )
        service(['zookeeper', 'kafka'], 'start')    # Mind!
        monitoring('enable')
    elif args.mode == 'clean':
        print "==== Starting at %s ====" % startTime
        cleanup_snapshots(zpoolName=args.pool, hours=args.time)

    print "==== Finished in: %s ====" % (datetime.datetime.now() - startTime)


if __name__ == "__main__":
    main()

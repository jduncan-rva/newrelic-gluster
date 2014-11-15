#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-
# Copyright (C) 2013  Jamie Duncan (jamie.e.duncan@gmail.com)

# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

# File Name : newrelic-gluster.py
# Creation Date : 11-14-2014
# Created By : Jamie Duncan
# Last Modified : Sat 15 Nov 2014 10:31:45 AM EST
# Purpose : A Gluster / RHS plugin for New Relic

import json
import psutil
import urllib2
import ConfigParser
import os
import sys
import time
from subprocess import Popen, PIPE
import logging
import socket
import _version

class NewRHELic:

    def __init__(self, conf='/etc/newrhelic.conf'):

        self.guid = 'com.rhel.gluster_statistics'
        self.name = 'Gluster Statistics'
        self.version = _version.__version__
        self.api_url = 'https://platform-api.newrelic.com/platform/v1/metrics'
        self.config_file = conf
        socket.setdefaulttimeout(5)

        # Store some system info
        self.uname = os.uname()
        self.pid = os.getpid()
        self.hostname = self.uname[1]  # This will likely be Linux-specific, but I don't want to load a whole module to get a hostname another way
        self.kernel = self.uname[2]
        self.arch = self.uname[4]

        self.json_data = dict()     # A construct to hold the json call data as we build it
        self.first_run = True   # This is set to False after the first run function is called

	#Various IO buffers
        self.buffers = {
        }

        # Open the config and log files in their own try/except
        try:
            config = ConfigParser.RawConfigParser()
            config.read(self.config_file)

            # Decide whether or not we are a gluster server or just a client
            self.is_server = config.get('gluster', 'server')

            logfilename = config.get('plugin','logfile')
            loglevel = config.get('plugin','loglevel').upper()
            logging.basicConfig(filename=logfilename,
                    level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(name)s:%(funcName)s: %(message)s',
                    )
            self.logger = logging.getLogger(__name__)
            if loglevel == "DEBUG":
                console = logging.StreamHandler()
                formatter = logging.Formatter('%(levelname)-8s %(name)s:%(funcName)s: %(message)s')
                console.setLevel(logging.DEBUG)
                console.setFormatter(formatter)
                self.logger.addHandler(console)
                self.logger.setLevel(logging.DEBUG)
            else:
                self.logger.setLevel(loglevel)

        except Exception, e:
            print "Unable to Get Going!"
            raise e

        try:
            self.license_key = config.get('site', 'key')
            self.pid_file = config.get('plugin', 'pidfile')
            self.interval = config.getint('plugin', 'interval')
            self.enable_proxy = config.getboolean('proxy','enable_proxy')

            if self.enable_proxy:
                proxy_host = config.get('proxy','proxy_host')
                proxy_port = config.get('proxy','proxy_port')
                # These proxy_setttings will be used by urllib2
                self.proxy_settings = {
                        'http': '%s:%s' % (proxy_host, proxy_port),
                        'https': '%s:%s' % (proxy_host, proxy_port)
                }
                self.logger.info("Configured to use proxy: %s:%s" % (proxy_host, proxy_port))


            # Create a dictionary to hold the various data metrics.
            self.metric_data = dict()

            # Build out the JSON Stanza
            self._build_agent_stanza()

        except Exception, e:
            self.logger.exception(e)
            raise e

    def _get_disk_utilization(self):
        '''This will return disk utilziation percentage for each mountpoint'''
        try:
            disks = psutil.disk_partitions(all=True)
            for p in disks:
                if p.fstype == 'fuse.glusterfs':
                    title = "Component/Gluster/%s/%s[percent]" % (p.device.replace('/','|'),p.mountpoint.replace('/','|'))
                    x = psutil.disk_usage(p.mountpoint)
                    self.metric_data[title] = x.percent
        except Exception, e:
            self.logger.exception(e)
            pass

    def _get_disk_stats(self):
        '''this will show system-wide disk statistics'''
        try:
            d = psutil.disk_io_counters()

            for i in range(len(d)):
                if d._fields[i] == 'read_time' or d._fields[i] == 'write_time':         #statistics come in multiple units from this output
                    title = "Component/Disk/Read-Write Time/%s[ms]" % d._fields[i]
                    val = d[i]
                elif d._fields[i] == 'read_count' or d._fields[i] == 'write_count':
                    title = "Component/Disk/Read-Write Count/%s[integer]" % d._fields[i]
                    val = d[i] - self.buffers[d._fields[i]]
                    self.buffers[d._fields[i]] = d[i]
                else:
                    title = "Component/Disk/IO/%s[bytes]" % d._fields[i]
                    val = d[i] - self.buffers[d._fields[i]]
                    self.buffers[d._fields[i]] = d[i]

                self.metric_data[title] = val
        except Exception, e:
            self.logger.exception(e)
            pass

    def _build_agent_stanza(self):
        '''this will build the 'agent' stanza of the new relic json call'''
        try:
            values = dict()
            values['host'] = self.hostname
            values['pid'] = self.pid
            values['version'] = self.version

            self.json_data['agent'] = values
        except Exception, e:
            self.logger.exception(e)
            raise e

    def _reset_json_data(self):
        '''this will 'reset' the json data structure and prepare for the next call. It does this by mimicing what happens in __init__'''
        try:
            self.metric_data = dict()
            self.json_data = dict()
            self._build_agent_stanza()
        except Exception, e:
            self.logger.exception(e)
            raise e

    def _build_component_stanza(self):
        '''this will build the 'component' stanza for the new relic json call'''
        try:
            c_list = list()
            c_dict = dict()
            c_dict['name'] = self.hostname
            c_dict['guid'] = self.guid
            c_dict['duration'] = self.interval

            self._get_disk_utilization()
            self._get_disk_stats()

            c_dict['metrics'] = self.metric_data
            c_list.append(c_dict)

            self.json_data['components'] = c_list
        except Exception, e:
            self.logger.exception(e)
            raise e

    def _prep_first_run(self):
        '''this will prime the needed buffers to present valid data when math is needed'''
        try:
            #create the first counter values to do math against for network, disk and swap

            #then we sleep so the math represents 1 minute intervals when we do it next
            self.logger.debug("sleeping...")
            time.sleep(60)
            self.first_run = False
            self.logger.debug("The pump is primed. Continuing to Run")

            return True
        except Exception, e:
            self.logger.exception(e)
            raise e

    def add_to_newrelic(self):
        '''this will glue it all together into a json request and execute'''
        if self.first_run:
            self._prep_first_run()  #prime the data buffers if it's the first loop

        self._build_component_stanza()  #get the data added up
        try:
            if self.enable_proxy:
                proxy_handler = urllib2.ProxyHandler(self.proxy_settings)
                opener = urllib2.build_opener(proxy_handler)
            else:
                opener = urllib2.build_opener(urllib2.HTTPHandler(), urllib2.HTTPSHandler())

            request = urllib2.Request(self.api_url)
            request.add_header("X-License-Key", self.license_key)
            request.add_header("Content-Type","application/json")
            request.add_header("Accept","application/json")

            response = opener.open(request, json.dumps(self.json_data))

            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug("%s (%s)" % (request.get_full_url(), response.getcode()))
                self.logger.debug(json.dumps(self.json_data))

            response.close()

        except urllib2.HTTPError, err:
            self.logger.error("HTTP Error: %s" % err)
            pass    #i know, i don't like it either, but we don't want a single failed connection to break the loop.

        except urllib2.URLError, err:
            self.logger.error("URL Error (DNS Error?): %s" % err)
            pass
        self._reset_json_data()

#!/usr/bin/env python
# Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
#
# Except as expressly permitted in a written agreement between you or your
# company and Hortonworks, Inc. or an authorized affiliate or partner
# thereof, any use, reproduction, modification, redistribution, sharing,
# lending or other exploitation of all or any part of the contents of this
# software is strictly prohibited.
#
import os
import sys
import errno
import zipfile
import ConfigParser
import socket
import subprocess

base_dir = ' '
webapp_dir = ''
java_bin = ''
jar_bin = ''
conf = ''
options = []
class_path = ''
log_dir = ''
pid_dir = ''
pid_file = ''
home_dir = ''
data_dir = ''
hostname = ''
heap = ''

def get_class_path(paths):
    separator = ';' if sys.platform == 'win32' else ':';
    return separator.join(paths)

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def resolve_sym_link(path):
    path = os.path.realpath(path)
    base_dir = os.path.dirname(os.path.dirname(path))
    return path, base_dir


def set_java_env():
    global java_bin, jar_bin
    JAVA_HOME = os.getenv('JAVA_HOME')
    if JAVA_HOME:
        java_bin = os.path.join(JAVA_HOME, 'bin', 'java')
        jar_bin = os.path.join(JAVA_HOME, 'bin', 'jar')
    else:
        os.sys.exit('java and jar commands are not available. Please configure'
                    ' JAVA_HOME')


def set_opts(opt, *env_vars):
    for env_var in env_vars:
        opt += ' ' + os.getenv(env_var, '')
    return opt.strip()


def init_client(webapp_dir):
    global options, class_path, conf, base_dir, heap
    cp = [conf]

    # Expand war for jars in WEB-INF/lib
    app_dir = os.path.join(webapp_dir, 'beacon')
    create_app_dir(webapp_dir, app_dir, 'beacon' + '.war')

    cp.append(get_hadoop_classpath())
    app_dirs = [app for app in os.listdir(webapp_dir) if os.path.isdir(os.path.join(webapp_dir, app))]
    for app in app_dirs:
        cp.append(os.path.join(webapp_dir, app, 'WEB-INF', 'lib', '*'))
    class_path = get_class_path(cp)
    options.extend([os.getenv('BEACON_CLIENT_OPTS'), os.getenv('BEACON_OPTS')])
    heap = os.getenv('BEACON_CLIENT_HEAP', '-Xmx1024m')


def init_server(webapp_dir):
    global options, class_path, log_dir, pid_dir, pid_file, data_dir, \
        home_dir, conf, base_dir, hostname, heap
    options.extend([os.getenv('BEACON_SERVER_OPTS'), os.getenv('BEACON_OPTS')])
    heap = os.getenv('BEACON_SERVER_HEAP', '-Xmx1024m')

    app_dir = os.path.join(webapp_dir, 'beacon')
    # Expand war for jars in WEB-INF/lib
    create_app_dir(webapp_dir, app_dir, 'beacon' + '.war')
    cp = [conf, os.path.join(app_dir, 'WEB-INF', 'lib', 'beacon-distcp.jar'),os.path.join(app_dir, 'WEB-INF', 'lib', 'javax.servlet-3.0.0.v201112011016.jar'),
          get_hadoop_classpath(),
          os.path.join(app_dir, 'WEB-INF', 'classes'),
          os.path.join(app_dir, 'WEB-INF', 'lib', '*')]

    class_path = get_class_path(cp)
    log_dir = os.getenv('BEACON_LOG_DIR', os.path.join(base_dir, 'logs'))
    pid_dir = os.getenv('BEACON_PID_DIR', os.path.join(base_dir, 'pids'))
    pid_file = os.path.join(pid_dir, 'beacon.pid')
    data_dir = os.getenv('BEACON_DATA_DIR', os.path.join(base_dir, 'data'))
    home_dir = os.getenv('BEACON_HOME_DIR', base_dir)
    hostname = os.getenv('HOSTNAME', socket.gethostname())
    #app_type = os.getenv('BEACON_APP_TYPE', app)


def get_hadoop_command():
    hadoop_script = 'hadoop'
    os_cmd = ['which']
    if sys.platform == 'win32':
        hadoop_script = 'hadoop.cmd'
        os_cmd = ['cmd', '/c', 'where']

    os_cmd.append(hadoop_script)
    p = subprocess.Popen(os_cmd, stdout=subprocess.PIPE)
    pout = p.communicate()[0]
    if pout :
        hadoop_command = pout.splitlines()[0]
        return hadoop_command

    # If which/where does not find hadoop command, derive
    # hadoop command from  HADOOP_HOME
    hadoop_home = os.getenv('HADOOP_HOME', None)
    if hadoop_home:
        hadoop_command = os.path.join(hadoop_home, 'bin', hadoop_script)
        if os.path.exists(hadoop_command) :
            return hadoop_command
    return None


def get_hadoop_classpath():
    global base_dir

    # Get hadoop class path from hadoop command
    hadoop_cmd = get_hadoop_command()
    #hadoop_cmd = None      #uncomment to run local
    if hadoop_cmd:
        p = subprocess.Popen([hadoop_cmd, 'classpath'], stdout=subprocess.PIPE)
        output = p.communicate()[0]
        return output.rstrip()
    # Use beacon hadoop libraries
    else:
        hadoop_libs = os.path.join(base_dir, 'hadooplibs', '*')
        print 'Could not find installed hadoop and HADOOP_HOME is not set or HADOOP_HOME/bin/hadoop doesn\'t exist.'
        print 'Using the default jars bundled in ' + hadoop_libs
        return hadoop_libs


def create_app_dir(webapp_dir, app_base_dir, app_war):
    app_webinf_dir = os.path.join(app_base_dir, 'WEB-INF')
    if not os.path.exists(app_webinf_dir):
        mkdir_p(app_base_dir)
        war_file = os.path.join(webapp_dir, app_war)
        zf = zipfile.ZipFile(war_file, 'r')
        zf.extractall(app_base_dir)


def init_beacon_env(conf):
    ini_file = os.path.join(conf, 'beacon_env.ini')
    config = ConfigParser.ConfigParser()
    config.optionxform = str
    config.read(ini_file)
    conf_environment = config.options('environment')
    for option in conf_environment:
        value = config.get('environment', option)
        os.environ[option] = value

def init_config(cmd, cmd_type):
    global base_dir, conf, options, webapp_dir

    # resolve links - $0 may be a soft link
    prg, base_dir = resolve_sym_link(os.path.abspath(cmd))
    webapp_dir = os.path.join(base_dir, 'server', 'webapp')

    conf = os.getenv('BEACON_CONF', os.path.join(base_dir, 'conf'))
    init_beacon_env(conf)
    set_java_env()

    if cmd_type == 'client':
        init_client(webapp_dir)
    elif cmd_type == 'server':
        expanded_webapp_dir = os.getenv('BEACON_EXPANDED_WEBAPP_DIR',
                                        webapp_dir)
	#print expanded_webapp_dir
        init_server(expanded_webapp_dir)
    else:
        os.sys.exit('Invalid option for type: ' + cmd_type)

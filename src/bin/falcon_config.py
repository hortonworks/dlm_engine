#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#
import os
import sys
import errno
import zipfile
import ConfigParser
import subprocess

base_dir = ' '
webapp_dir = ''
java_bin = ''
jar_bin = ''
conf = ''
options = ''
class_path = ''
log_dir = ''
pid_file = ''
home_dir = ''
data_dir = ''

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
    global options, class_path, conf, base_dir
    cp = [conf]
    client_lib = os.path.join(base_dir, 'client', 'lib', '*')
    cp.append(client_lib)
    app_dirs = [app for app in os.listdir(webapp_dir) if os.path.isdir(os.path.join(webapp_dir, app))]
    for app in app_dirs:
        cp.append(os.path.join(webapp_dir, app, 'WEB_INF', 'lib', '*'))
    class_path = get_class_path(cp)
    options = set_opts(options, 'FALCON_CLIENT_OPTS', 'FALCON_CLIENT_HEAP')


def init_server(app_type, webapp_dir):
    global options, class_path, log_dir, pid_file, data_dir, \
        home_dir, conf, base_dir
    if app_type == 'prism':
        options = set_opts(options, 'FALCON_PRISM_OPTS',
                           'FALCON_PRISM_HEAP')
    elif app_type == 'falcon':
        options = set_opts(options, 'FALCON_SERVER_OPTS',
                           'FALCON_SERVER_HEAP')
    else:
        os.sys.exit('Invalid option for app ' + app_type +
                    '. Valid choices are falcon and prism')

    app_dir = os.path.join(webapp_dir, app_type)
    create_app_dir(webapp_dir, app_dir, app_type + '.war')
    cp = [conf, get_hadoop_classpath(),
          os.path.join(app_dir, 'WEB-INF', 'classes'),
          os.path.join(app_dir, 'WEB-INF', 'lib', '*'),
          os.path.join(base_dir, 'libext', '*')]
    class_path = get_class_path(cp)
    log_dir = os.getenv('FALCON_LOG_DIR', os.path.join(base_dir, 'logs'))
    pid_dir = os.getenv('FALCON_PID_DIR', log_dir)
    pid_file = os.path.join(pid_dir, app_type + '.pid')
    data_dir = os.getenv('FALCON_DATA_DIR', os.path.join(log_dir, 'data'))
    home_dir = os.getenv('FALCON_HOME_DIR', base_dir)


def get_hadoop_command():
    hadoop_script = 'hadoop'
    os_cmd = ['which']
    if sys.platform == 'win32':
        hadoop_script = 'hadoop.cmd'
        os_cmd = ['cmd', '/c', 'where']

    os_cmd.append(hadoop_script)
    p = subprocess.Popen(os_cmd, stdout=subprocess.PIPE)
    hadoop_command = p.communicate()[0].splitlines()[0]
    # If which/where does not find hadoop command, derive
    # hadoop command from  HADOOP_HOME
    if hadoop_command:
        return hadoop_command

    hadoop_home = os.getenv('HADOOP_HOME', None)
    if not hadoop_home:
        return None

    return os.path.join(hadoop_home, 'bin', hadoop_script)


def get_hadoop_classpath():
    global base_dir

    # Get hadoop class path from hadoop command
    hadoop_cmd = get_hadoop_command()
    if hadoop_cmd:
        p = subprocess.Popen([hadoop_cmd, 'classpath'], stdout=subprocess.PIPE)
        output = p.communicate()[0]
        return output.rstrip()
    # Use falcon hadoop libraries
    else:
        hadoop_libs = os.path.join(base_dir, 'hadooplibs', '*')
        print 'Could not find installed hadoop and HADOOP_HOME is not set.'
        print 'Using the default jars bundled in ' + hadoop_libs
        return hadoop_libs


def create_app_dir(webapp_dir, app_base_dir, app_war):
    app_webinf_dir = os.path.join(app_base_dir, 'WEB-INF')
    if not os.path.exists(app_webinf_dir):
        mkdir_p(app_base_dir)
        war_file = os.path.join(webapp_dir, app_war)
        zf = zipfile.ZipFile(war_file, 'r')
        zf.extractall(app_base_dir)


def init_falcon_env(conf):
    ini_file = os.path.join(conf, 'falcon_env.ini')
    config = ConfigParser.ConfigParser()
    config.optionxform = str
    config.read(ini_file)
    options = config.options('environment')
    for option in options:
        value = config.get('environment', option)
        os.environ[option] = value

def init_config(cmd, cmd_type, app_type=None):
    global base_dir, conf, options, webapp_dir
    options = '-Xmx1024m ' + os.getenv('FALCON_OPTS', '')

    # resolve links - $0 may be a soft link
    prg, base_dir = resolve_sym_link(os.path.abspath(cmd))
    webapp_dir = os.path.join(base_dir, 'server', 'webapp')

    conf = os.getenv('FALCON_CONF', os.path.join(base_dir, 'conf'))
    init_falcon_env(conf)
    set_java_env()

    if cmd_type == 'client':
        init_client(webapp_dir)
    elif cmd_type == 'server':
        expanded_webapp_dir = os.getenv('FALCON_EXPANDED_WEBAPP_DIR',
                                        webapp_dir)
        init_server(app_type, expanded_webapp_dir)
    else:
        os.sys.exit('Invalid option for type: ' + cmd_type)

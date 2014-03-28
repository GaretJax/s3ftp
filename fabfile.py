from os.path import join, dirname
from glob import glob
from fabric.api import env


env.package_name = 's3ftp'
env.user = ''
env.hosts = [

]

# Assets pipeline configuration (may not be needed if the project does not
# make use of assets and/or static files for web development)
env.assets_dir = join(env.package_name, 'assets')
env.static_dir = join(env.package_name, 'static')
env.templates_dir = join(env.package_name, 'templates')


for f in glob(join(dirname(__file__), 'fabtasks', '*.py')):
    execfile(f)
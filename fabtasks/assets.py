import os
from livereload import Server
from fabric.api import task, local, env


@task
def watch_styles():
    config = os.path.join(env.assets_dir, 'sass', 'config.rb')
    local('compass watch -c {}'.format(config))


@task
def compile_styles():
    config = os.path.join(env.assets_dir, 'sass', 'config.rb')
    local('compass compile -c {}'.format(config))


@task
def watch_scripts():
    output = os.path.join(env.static_dir, 'scripts', 'master.js')
    scripts = os.path.join(env.assets_dir, 'coffeescripts')
    local('coffee -c -w -j {} {}'.format(output, scripts))


@task
def compile_scripts():
    output = os.path.join(env.static_dir, 'scripts', 'master.js')
    scripts = os.path.join(env.assets_dir, 'coffeescripts')
    local('coffee -c -j {} {}'.format(output, scripts))


@task
def livereload():
    url = 'http://dev:35729/livereload.js'
    print (
        'To livereload your web pages add the following script tag to '
        'your HTML:'
    )
    print ''
    print '    <script type="text/javascript" src="{}"></script>'.format(url)
    print ''
    server = Server()
    server.watch(env.static_dir)
    server.watch(env.templates_dir)
    server.serve(port=35729, host='0.0.0.0')
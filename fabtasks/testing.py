from fabric.api import task, local


@task
def test():
    local('py.test')
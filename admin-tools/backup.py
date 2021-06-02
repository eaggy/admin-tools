#!/usr/bin/python3
"""Definition of functions required to backup data."""

import os
import datetime
import configparser
import socket
from setup_logger import logger

config = configparser.ConfigParser()
config.read('/home/backuper/backup_settings.cfg')

BACKUP_PATHS = config['BACKUP']['LOCATION']

DATABASE_MYSQL = config['DB_MYSQL']['DATABASE']
DATABASE_POSTGRESQL = config['DB_POSTGRESQL']['DATABASE']
PG_PASSWORD = config['DB_POSTGRESQL']['PASSWORD']

SERVER = config['DESTINATION']['SERVER']
LOCATION = config['DESTINATION']['LOCATION']
USER = config['DESTINATION']['USER']
PASSWORD = config['DESTINATION']['PASSWORD']


def backup_files(paths, user, server, location):
    """
    Make backup of files and folders and transfer data to remote server.

    Parameters
    ----------
    paths : str
        Comma-separated collection of pathes of folders and files to backup.
    user : str
        User to connect the remote server over SSH.
    server : str
        IP of the remote server.
    location : str
        Path to the backup location on the remote server.

    Returns
    -------
    None.

    """
    try:
        for backup_path in paths.split(','):
            backup_path = backup_path.strip()
            now = datetime.datetime.now().strftime('%Y%m%d')
            file_location = '/tmp/{}-{}-{}.tar.gz'.format(
                now, socket.gethostname(), backup_path.replace('/', '-')[1:])

            # create temporarily data store
            os.system('tar -cpzf {} {}'.format(file_location, backup_path))

            # transfer data to remote server
            os.system("rsync -avzhe  'ssh -i /root/.ssh/id_rsa' {} {}@{}:{}"
                      .format(file_location, user, server, location))

            # delete temporarily data store
            os.system('rm -f {}'.format(file_location))

    except Exception as e:
        logger.error(e)


def dump_db(databases, db_type, user, server, location, db_password=''):
    """
    Dump database and transfer data to remote server.

    Parameters
    ----------
    databases : str
        Comma-separated collection of database names to backup.
    db_type : str
        Database type: `MySQL` or `PostgreSQL`.
    user : str
        User to connect the remote server over SSH.
    server : str
        IP of the remote server.
    location : str
        Path to the backup location on the remote server.
    db_password : str, optional
        Database password. The default is ''.

    Returns
    -------
    None.

    """
    try:
        for database in databases.split(','):
            database = database.strip()
            now = datetime.datetime.now().strftime('%Y%m%d')
            file_location = '/tmp/{}-{}-{}-dump.sql'.format(
                now, socket.gethostname(), database)
            if db_type.lower() == 'mysql':

                # dump database
                os.system('mysqldump --databases {} > {}'
                          .format(database, file_location))
            elif db_type.lower() == 'postgresql':

                # dump database
                os.system('PGPASSWORD={} pg_dump -Fc -U postgres {} > {}'
                          .format(db_password, database, file_location))
            else:
                return None

            # transfer data to remote server
            os.system("rsync -avzhe  'ssh -i /root/.ssh/id_rsa' {} {}@{}:{}"
                      .format(file_location, user, server, location))

            # delete temporarily data store
            os.system('rm -f {}'.format(file_location))

    except Exception as e:
        logger.error(e)


def main():
    """
    Make backup of files and databases.

    Returns
    -------
    None.

    """
    # make backup of files
    backup_files(BACKUP_PATHS, USER, SERVER, LOCATION)

    # make backup of database
    if DATABASE_MYSQL:
        dump_db(DATABASE_MYSQL, 'MySQL', USER, SERVER, LOCATION)
    if DATABASE_POSTGRESQL:
        dump_db(DATABASE_POSTGRESQL, 'PostgreSQL', USER, SERVER, LOCATION,
                PG_PASSWORD)


if __name__ == '__main__':
    main()

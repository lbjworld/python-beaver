# -*- coding: utf-8 -*-
import Queue
import signal
import sys
import time

from beaver.transports import create_transport
from beaver.transports.exception import TransportException
from unicode_dammit import unicode_dammit

class ChkPtMgr:
    def __init__(self, beaver_config):
        self._file_chkpt = dict()
        self._beaver_config = beaver_config

    def need_do_checkpoint(self, filename, last_update_time):
        if not self._file_chkpt.get(filename):
            sincedb_write_interval = self._beaver_config.get_field('sincedb_write_interval', filename)
            self._file_chkpt[filename] = (0, sincedb_write_interval)
            return True
        last_checkpoint_time, sincedb_write_interval = self._file_chkpt[filename]
        if last_update_time - last_checkpoint_time > sincedb_write_interval:
            return True
        return False

    def update_checkpoint_time(self, filename, new_update_time):
        if self._file_chkpt.get(filename):
            _, sincedb_write_interval = self._file_chkpt[filename]
            self._file_chkpt[filename] = (new_update_time, sincedb_write_interval)



def run_queue(queue, beaver_config, logger=None):
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGQUIT, signal.SIG_DFL)

    # record all checkpoints of files
    chkpt_mgr = ChkPtMgr(beaver_config)

    last_update_time = int(time.time())
    queue_timeout = beaver_config.get('queue_timeout')
    wait_timeout = beaver_config.get('wait_timeout')

    transport = None
    try:
        logger.debug('Logging using the {0} transport'.format(beaver_config.get('transport')))
        transport = create_transport(beaver_config, logger=logger)

        failure_count = 0
        while True:
            if not transport.valid():
                logger.info('Transport connection issues, stopping queue')
                break

            if int(time.time()) - last_update_time > queue_timeout:
                logger.info('Queue timeout of "{0}" seconds exceeded, stopping queue'.format(queue_timeout))
                break

            try:
                command, data = queue.get(block=True, timeout=wait_timeout)
                if command == "callback":
                    last_update_time = int(time.time())
                    logger.debug('Last update time now {0}'.format(last_update_time))
            except Queue.Empty:
                logger.debug('No data')
                continue

            if command == 'callback':
                if data.get('ignore_empty', False):
                    logger.debug('removing empty lines')
                    lines = data['lines']
                    new_lines = []
                    for line in lines:
                        message = unicode_dammit(line)
                        if len(message) == 0:
                            continue
                        new_lines.append(message)
                    data['lines'] = new_lines

                if len(data['lines']) == 0:
                    logger.debug('0 active lines sent from worker')
                    continue

                while True:
                    try:
                        transport.callback(**data)
                        # sync in sincedb
                        if data.get('filename') and data.get('offset') and chkpt_mgr.need_do_checkpoint(data['filename'], last_update_time):
                            transport.checkpoint(data['filename'], data['offset'])
                            chkpt_mgr.update_checkpoint_time(data['filename'], last_update_time)
                            logger.info('Write checkpoint({file}, {offset}) into sincedb'.format(file=data['filename'], offset=data['offset']))
                        break
                    except TransportException:
                        failure_count = failure_count + 1
                        if failure_count > beaver_config.get('max_failure'):
                            failure_count = beaver_config.get('max_failure')

                        sleep_time = beaver_config.get('respawn_delay') ** failure_count
                        logger.info('Caught transport exception, reconnecting in %d seconds' % sleep_time)

                        try:
                            transport.invalidate()
                            time.sleep(sleep_time)
                            transport.reconnect()
                            if transport.valid():
                                failure_count = 0
                                logger.info('Reconnected successfully')
                        except KeyboardInterrupt:
                            logger.info('User cancelled respawn.')
                            transport.interrupt()
                            sys.exit(0)
            elif command == 'addglob':
                beaver_config.addglob(*data)
                transport.addglob(*data)
            elif command == 'exit':
                break
    except KeyboardInterrupt:
        logger.debug('Queue Interruped')
        if transport is not None:
            transport.interrupt()

        logger.debug('Queue Shutdown')

from enum import Enum
from hashlib import md5
from random import randbytes
from datetime import datetime
from threading import Lock, Thread
from typing import List, Callable
from time import sleep
from threading import Thread


class WebQueriedWorkerStatus(Enum):
    Pending = 0
    Running = 1
    Finished = 2
    Stopping = 3
    Stopped = 4
    Failed = 5


class WebQueriedWorker:
    def __init__(self, thread_func: Callable = None, name: str = 'WebQueriedWorker'):
        self.name = name
        self.id = md5(randbytes(256)).hexdigest()
        self.create_date = datetime.now()
        self.start_date = None
        self.finish_date = None
        self._runtime_status = None

        self.stoppable = False  # this must be set to True ONLY if WebQueriedWorkerStatus.Stopping check implemented in main()!!!
        
        if thread_func is None:
            self.worker = Thread(target=self.main)
        else:
            self.worker = Thread(target=thread_func)
        
        self.worker_log = ''
        self.worker_log_lock = Lock()

        self.runtime_status = WebQueriedWorkerStatus.Pending

    def write_to_log(self, message: str):
        self.worker_log_lock.acquire()
        self.worker_log += message + '\n'
        self.worker_log_lock.release()
    
    @property
    def runtime_status(self):
        return self._runtime_status
    
    @runtime_status.setter
    def runtime_status(self, value: WebQueriedWorkerStatus):
        match value.name:
            case 'Pending':
                if self._runtime_status is None:
                    self._runtime_status = WebQueriedWorkerStatus.Pending
                    self.create_date = datetime.now()
                    self.write_to_log(f'WebQueriedWorker created at {self.create_date}')
                else:
                    raise Exception('cant set Pending status on existing worker')
            case 'Running':
                if self._runtime_status == WebQueriedWorkerStatus.Pending:
                    self._runtime_status = WebQueriedWorkerStatus.Running
                    self.start_date = datetime.now()
                    self.write_to_log(f'WebQueriedWorker started at {self.start_date}')
                else:
                    raise Exception('cant start non Pending worker')
            case 'Finished':
                if self._runtime_status == WebQueriedWorkerStatus.Running:
                    self._runtime_status = WebQueriedWorkerStatus.Finished
                    self.finish_date = datetime.now()
                    self.write_to_log(f'WebQueriedWorker finished at {self.finish_date}')
                else:
                    raise Exception('cant finish non Running worker')
            case 'Stopping':
                if self.stoppable:
                    if self._runtime_status == WebQueriedWorkerStatus.Running:
                        self._runtime_status = WebQueriedWorkerStatus.Stopping
                        self.write_to_log('WebQueriedWorker received stop signal!')
                    else:
                        raise Exception('cant stop non Running worker')
                else:
                    raise Exception('this worker cannot be stopped')
            case 'Stopped':
                if self._runtime_status == WebQueriedWorkerStatus.Stopping:
                    self._runtime_status = WebQueriedWorkerStatus.Stopped
                    self.finish_date = datetime.now()
                    self.write_to_log(f'WebQueriedWorker stopped at {self.finish_date}')
                else:
                    raise Exception('cant set status Stopped on non Stopping worker')
            case 'Failed':
                if self._runtime_status == WebQueriedWorkerStatus.Running:
                    self._runtime_status = WebQueriedWorkerStatus.Failed
                    self.finish_date = datetime.now()
                    self.write_to_log(f'WebQueriedWorker failed at {self.finish_date}')
                else:
                    raise Exception('cant fail non Running worker')
            case _:
                pass

    @property
    def status(self):
        return self.runtime_status.name

    @property
    def log(self):
        self.worker_log_lock.acquire()
        ret = self.worker_log
        self.worker_log_lock.release()
        return ret

    def start(self):
        try:
            self.runtime_status = WebQueriedWorkerStatus.Running
            self.worker.start()
        except Exception as e:
            self.runtime_status = WebQueriedWorkerStatus.Failed
            self.write_to_log(f'Exception raised on start():\n{str(e)}')

    def stop(self):
        """
        Для использования этого метода в main() должна быть определена проверка на статус Stopping
        """
        self.runtime_status = WebQueriedWorkerStatus.Stopping
        self.worker.join()
        self.runtime_status = WebQueriedWorkerStatus.Stopped

    def main(self):
        pass


class WebQueriedWorkerPool:
    def __init__(self, max_running_workers: int = 2):
        self._workers: List[WebQueriedWorker] = []

        self.resource_lock = Lock()

        self.max_running_workers = max_running_workers
        self.pending_starter = Thread(target=self._start_pending)
        self.pending_starter_running = True
        self.pending_starter.start()

    def stop(self):
        self.pending_starter_running = False
        self.pending_starter.join()

    def workers(self):
        self.resource_lock.acquire()
        ret = self._workers
        self.resource_lock.release()
        return ret

    def worker_by_id(self, worker_id: str):
        try:
            self.resource_lock.acquire()
            worker = next(filter(lambda x: x.id == worker_id, self._workers))
            self.resource_lock.release()
            return worker
        except StopIteration:
            self.resource_lock.release()
            raise FileNotFoundError()
        
    def add_worker(self, worker: WebQueriedWorker):
        self.resource_lock.acquire()
        self._workers.append(worker)
        self.resource_lock.release()

    def delete_worker(self, worker_id: str):
        worker = self.worker_by_id(worker_id)
        if worker.runtime_status in [
            WebQueriedWorkerStatus.Finished, WebQueriedWorkerStatus.Stopped, WebQueriedWorkerStatus.Failed, WebQueriedWorkerStatus.Pending
        ]:
            self.resource_lock.acquire()
            self._workers.remove(worker)
            self.resource_lock.release()
        else:
            raise Exception('can remove only non Running workers')

    def _first_pending(self):
        pending_workers_ids = [x.id for x in self.workers() if x.status == 'Pending']
        if len(pending_workers_ids) == 0:
            return None
        else:
            return pending_workers_ids[0]
    
    def _running_count(self):
        return len([x for x in self.workers() if x.status == 'Running'])
    
    def _start_pending(self):
        while self.pending_starter_running:
            sleep(3.)
            if self.max_running_workers <= self._running_count():
                continue

            pending_worker_id = self._first_pending()
            if pending_worker_id is None:
                continue

            self.worker_by_id(pending_worker_id).start()

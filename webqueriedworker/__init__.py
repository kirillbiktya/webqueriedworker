from enum import Enum
from hashlib import md5
from random import randbytes
from datetime import datetime
from typing import List, Callable
from time import sleep
from threading import Thread, Event, Lock


class StoppedException(Exception):
    pass


class WebQueriedWorkerStatus(Enum):
    Pending = 0
    Running = 1
    Finished = 2
    Stopping = 3
    Stopped = 4
    Cancelled = 5
    Failed = 6
    PartiallyFailed = 7


WORKER_BAD_STATUSES = {
    WebQueriedWorkerStatus.Stopping, 
    WebQueriedWorkerStatus.Stopped, 
    WebQueriedWorkerStatus.Cancelled, 
    WebQueriedWorkerStatus.Failed, 
    WebQueriedWorkerStatus.PartiallyFailed
}


class WebQueriedWorker:
    def __init__(
            self, 
            worker_pool: 'WebQueriedWorkerPool',
            thread_func: Callable = None, 
            name: str = 'WebQueriedWorker', 
            parent: str = None, 
            after: List[str] = [], 
            childs: List[str] = []
        ):
        self.worker_pool = worker_pool
        self.name = name
        self.id = md5(randbytes(256)).hexdigest()
        self.create_date = datetime.now()
        self.start_date = None
        self.finish_date = None
        self._runtime_status = None

        if thread_func is None:
            self.worker = Thread(target=self.main)
        else:
            self.worker = Thread(target=thread_func)
        
        self.worker_log = ''
        self.worker_log_lock = Lock()

        self.runtime_status = WebQueriedWorkerStatus.Pending

        self.parent = parent
        self.after = after
        self.childs = childs

        self.stop_event = Event()

    def to_dict(self):
        with self.worker_log_lock:
            ret = {
                'id': self.id,
                'name': self.name,
                'status': self.status,
                'create_date': str(self.create_date), 
                'start_date': str(self.start_date), 
                'finish_date': str(self.finish_date),
                'log': self.log,
                'after': [x.to_dict() for x in self.worker_pool.workers_by_id(self.after)],
                'childs': [x.to_dict() for x in self.worker_pool.workers_by_id(self.childs)]
            }
        return ret

    def _should_i_stop_myself(self):
        if self.stop_event.is_set():
            self.runtime_status = WebQueriedWorkerStatus.Stopping
            raise StoppedException()

    def write_to_log(self, message: str):
        with self.worker_log_lock:
            self.worker_log += f'{str(datetime.now())}: {message}\n'
    
    @property
    def is_child(self):
        return self.parent is not None
    
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
                    self.write_to_log(f'Создан процесс {self.name}')
                else:
                    raise Exception('Нельзя установить статус "Ожидает"')
            case 'Running':
                if self._runtime_status == WebQueriedWorkerStatus.Pending:
                    self._runtime_status = WebQueriedWorkerStatus.Running
                    self.start_date = datetime.now()
                    self.write_to_log(f'Запущен процесс {self.name}')
                else:
                    raise Exception('Нельзя запустить процесс вне статуса "Ожидает"')
            case 'Finished':
                if self._runtime_status == WebQueriedWorkerStatus.Running:
                    self._runtime_status = WebQueriedWorkerStatus.Finished
                    self.finish_date = datetime.now()
                    self.write_to_log(f'Завершен процесс {self.name}')
                    del self.worker
                else:
                    raise Exception('Нельзя завершить процесс вне статуса "Работает"')
            case 'Stopping':
                if self._runtime_status == WebQueriedWorkerStatus.Running:
                    self._runtime_status = WebQueriedWorkerStatus.Stopping
                    self.write_to_log(f'Процесс {self.name} получил стоп сигнал!')
                else:
                    raise Exception('Нельзя остановить процесс вне статуса "Работает"')
            case 'Stopped':
                if self._runtime_status == WebQueriedWorkerStatus.Stopping:
                    self._runtime_status = WebQueriedWorkerStatus.Stopped
                    self.finish_date = datetime.now()
                    self.write_to_log(f'Остановлен процесс {self.name}')
                    del self.worker
                else:
                    raise Exception('Нельзя установить статус "Остановлен" процессу вне статуса "Останавливается"')
            case 'Failed':
                if self._runtime_status in {WebQueriedWorkerStatus.Running, WebQueriedWorkerStatus.Pending}:
                    self._runtime_status = WebQueriedWorkerStatus.Failed
                    self.finish_date = datetime.now()
                    self.write_to_log(f'Процесс {self.name} завершился с необрабатываемой ошибкой')
                    del self.worker
                else:
                    raise Exception('Нельзя установить статус "Ошибка" процессу вне статуса "Работает"')
            case 'PartiallyFailed':
                if self._runtime_status == WebQueriedWorkerStatus.Running:
                    self._runtime_status = WebQueriedWorkerStatus.PartiallyFailed
                    self.finish_date = datetime.now()
                    self.write_to_log(f'Процесс {self.name} завершился с ошибками')
                    del self.worker
                else:
                    raise Exception('Нельзя установить статус "Есть ошибки" процессу вне статуса "Работает"')
            case 'Cancelled':
                if self._runtime_status == WebQueriedWorkerStatus.Pending:
                    self._runtime_status = WebQueriedWorkerStatus.Cancelled
                    self.finish_date = datetime.now()
                    self.write_to_log(f'Процесс {self.name} отменен из-за ошибок связанных процессов')
                    del self.worker
                else:
                    raise Exception('Нельзя установить статус "Отменен" процессу вне статуса "Ожидает"')
            case _:
                pass

    @property
    def status(self):
        return self.runtime_status.name

    @property
    def log(self):
        with self.worker_log_lock:
            ret = self.worker_log
        return ret

    def start(self):
        try:
            self.runtime_status = WebQueriedWorkerStatus.Running
            self.worker.start()
        except Exception as e:
            self.runtime_status = WebQueriedWorkerStatus.Failed
            self.write_to_log(f'Ошибка при запуске процесса:\n{str(e)}')

    def stop(self):
        if self.runtime_status == WebQueriedWorkerStatus.Running:
            self.stop_event.set()
        else:
            raise Exception('Процесс не может быть остановлен в данном статусе!')

    def cancel(self):
        self.runtime_status = WebQueriedWorkerStatus.Cancelled

    def main(self):
        pass


class WebQueriedWorkerPool:
    def __init__(self, max_running_workers: int = 10):
        self._workers: List[WebQueriedWorker] = []

        self.resource_lock = Lock()

        self.max_running_workers = max_running_workers
        self.pending_starter = Thread(target=self._start_pending)
        self.pending_starter_running = True
        self.pending_starter.start()

    def stop(self):
        self.pending_starter_running = False
        self.pending_starter.join()

    #region worker crud
    def workers(self):
        with self.resource_lock:
            ret = self._workers
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
        
    def workers_by_id(self, worker_ids: List[str]):
        with self.resource_lock:
            worker = list(filter(lambda x: x.id in worker_ids, self._workers))
        return worker
        
    def add_worker(self, worker: WebQueriedWorker):
        with self.resource_lock:
            self._workers.append(worker)
    
    def add_workers(self, workers: List[WebQueriedWorker]):
        with self.resource_lock:
            self._workers.extend(workers)

    def delete_worker(self, worker_id: str):
        worker = self.worker_by_id(worker_id)
        if worker.runtime_status in [
            WebQueriedWorkerStatus.Failed, 
            WebQueriedWorkerStatus.Cancelled, 
            WebQueriedWorkerStatus.Stopped, 
            WebQueriedWorkerStatus.Finished, 
            WebQueriedWorkerStatus.PartiallyFailed
        ]:
            with self.resource_lock:
                self._workers.remove(worker)
        else:
            raise Exception('Можно удалить процесс только в статусах "Ошибка", "Отменен", "Остановлен", "Завершен", "Есть ошибки"')
    
    def _pending_workers(self):
        pending_workers = [x for x in self.workers() if x.status == 'Pending']
        return pending_workers
    
    @property
    def running_count(self):
        return len([x for x in self.workers() if x.status == 'Running'])
    #endregion

    def _check_worker_dependencies_before_start(self, worker: WebQueriedWorker):
        if worker.after:
            after_workers = self.workers_by_id(worker.after)
            if any([x.runtime_status in WORKER_BAD_STATUSES for x in after_workers]):
                return 'cancel'
            
            if any([x.runtime_status != WebQueriedWorkerStatus.Finished for x in after_workers]):
                return 'wait'
        
        return 'run'

    #region worker-invoked checks
    def worker_should_partially_fail(self, worker: WebQueriedWorker):
        if worker.childs:
            child_workers = self.workers_by_id(worker.childs)
            if any([x.runtime_status in WORKER_BAD_STATUSES for x in child_workers]):
                return True
        return False
    #endregion
    
    def _start_pending(self):
        while self.pending_starter_running:
            sleep(3.)
            pending_workers = self._pending_workers()
            for worker in pending_workers:
                if self.running_count >= self.max_running_workers:
                    break

                action = self._check_worker_dependencies_before_start(worker)
                match action:
                    case 'wait':
                        continue
                    case 'cancel':
                        worker.cancel()
                        continue
                    case 'run':
                        worker.start()
                        continue
                    case _:
                        continue

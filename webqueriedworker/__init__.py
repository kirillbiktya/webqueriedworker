import multiprocessing
from enum import Enum
from time import sleep
from hashlib import md5
from random import randbytes
from datetime import datetime
from queue import Empty
import asyncio
from typing import Literal


class WebQueriedWorkerStatus(Enum):
    Initialization = -1
    Pending = 0
    Running = 1
    Finished = 2
    Cancelled = 3
    Failed = 4
    PartiallyFailed = 5


WORKER_BAD_STATUSES = {
    WebQueriedWorkerStatus.Cancelled, 
    WebQueriedWorkerStatus.Failed, 
    WebQueriedWorkerStatus.PartiallyFailed
}


class WebQueriedWorker:
    def __init__(
            self,
            name: str = 'WebQueriedWorker',
            parent: str | None = None,
            after: list[str] | None = None,
            childs: list[str] | None = None
        ):
        self._process: multiprocessing.Process = None
        self._manager = multiprocessing.Manager()
        self._shared_data = self._manager.dict()
        self._log_queue = multiprocessing.Queue()
        self._finished_by_pool = False

        self._shared_data['progress'] = -1.
        self._shared_data['status'] = WebQueriedWorkerStatus.Initialization.value
        self._shared_data['create_date'] = -1.
        self._shared_data['start_date'] = -1.
        self._shared_data['finish_date'] = -1.
        self._shared_data['childs'] = self._manager.list()
        self._shared_data['after'] = self._manager.list()

        self._worker_log = ''

        self.name = name
        self.id = md5(randbytes(16)).hexdigest()

        self.parent = parent
        self.after = after or []
        self.childs = childs or []

        self.runtime_status = WebQueriedWorkerStatus.Pending

    def __del__(self):
        self._manager.shutdown()

    @property
    def after(self) -> list[str]:
        return self._shared_data['after']
    
    @after.setter
    def after(self, value: list[str]):
        for after in value:
            self._shared_data['after'].append(after)

    @property
    def childs(self) -> list[str]:
        return self._shared_data['childs']
    
    @childs.setter
    def childs(self, value: list[str]):
        for child in value:
            self._shared_data['childs'].append(child)

    @property
    def create_date(self) -> datetime:
        if self._shared_data['create_date'] < 0:
            return None
        else:
            return datetime.fromtimestamp(self._shared_data['create_date'])

    @create_date.setter
    def create_date(self, value: datetime):
        self._shared_data['create_date'] = value.timestamp()
    
    @property
    def start_date(self) -> datetime:
        if self._shared_data['start_date'] < 0:
            return None
        else:
            return datetime.fromtimestamp(self._shared_data['start_date'])
    
    @start_date.setter
    def start_date(self, value: datetime):
        self._shared_data['start_date'] = value.timestamp()

    @property
    def finish_date(self) -> datetime:
        if self._shared_data['finish_date'] < 0:
            return None
        else:
            return datetime.fromtimestamp(self._shared_data['finish_date'])
    
    @finish_date.setter
    def finish_date(self, value: datetime):
        self._shared_data['finish_date'] = value.timestamp()

    @property
    def runtime_status(self) -> WebQueriedWorkerStatus:
        return WebQueriedWorkerStatus(self._shared_data['status'])
    
    @runtime_status.setter
    def runtime_status(self, value: WebQueriedWorkerStatus):
        # self._shared_data['status'] = value.value
        match value.name:
            case 'Pending':
                if self.runtime_status == WebQueriedWorkerStatus.Initialization:
                    self._shared_data['status'] = WebQueriedWorkerStatus.Pending.value
                    self.create_date = datetime.now()
                    self.write_to_log(f'Создан процесс {self.name}')
                else:
                    raise Exception('Нельзя установить статус "Ожидает"')
            case 'Running':
                if self.runtime_status == WebQueriedWorkerStatus.Pending:
                    self._shared_data['status'] = WebQueriedWorkerStatus.Running
                    self.start_date = datetime.now()
                    self.write_to_log(f'Запущен процесс {self.name}')
                else:
                    raise Exception('Нельзя запустить процесс вне статуса "Ожидает"')
            case 'Finished':
                if self.runtime_status == WebQueriedWorkerStatus.Running:
                    self._shared_data['status'] = WebQueriedWorkerStatus.Finished
                    self.finish_date = datetime.now()
                    self.write_to_log(f'Завершен процесс {self.name}')
                else:
                    raise Exception('Нельзя завершить процесс вне статуса "Работает"')
            case 'Failed':
                if self.runtime_status in {WebQueriedWorkerStatus.Running, WebQueriedWorkerStatus.Pending}:
                    self._shared_data['status'] = WebQueriedWorkerStatus.Failed
                    self.finish_date = datetime.now()
                    self.write_to_log(f'Процесс {self.name} завершился с необрабатываемой ошибкой')
                else:
                    raise Exception('Нельзя установить статус "Ошибка" процессу вне статуса "Работает"')
            case 'PartiallyFailed':
                if self.runtime_status == WebQueriedWorkerStatus.Running:
                    self._shared_data['status'] = WebQueriedWorkerStatus.PartiallyFailed
                    self.finish_date = datetime.now()
                    self.write_to_log(f'Процесс {self.name} завершился с ошибками')
                else:
                    raise Exception('Нельзя установить статус "Есть ошибки" процессу вне статуса "Работает"')
            case 'Cancelled':
                if self.runtime_status == WebQueriedWorkerStatus.Pending:
                    self._shared_data['status'] = WebQueriedWorkerStatus.Cancelled
                    self.finish_date = datetime.now()
                    self.write_to_log(f'Процесс {self.name} отменен из-за ошибок связанных процессов')
                else:
                    raise Exception('Нельзя установить статус "Отменен" процессу вне статуса "Ожидает"')
            case _:
                pass
    
    @property
    def status(self) -> str:
        return self.runtime_status.name

    @property
    def progress(self) -> float:
        return self._shared_data['progress']
    
    @progress.setter
    def progress(self, value: float):
        self._shared_data['progress'] = value

    @property
    def log(self) -> str:
        if self._log_queue.empty():
            return self._worker_log
        else:
            while not self._log_queue.empty():
                try:
                    self._worker_log += self._log_queue.get(block=False)
                except Empty:
                    break

            return self._worker_log

    def write_to_log(self, message: str):
        self._log_queue.put(f'{datetime.now().isoformat()}: {message}\n')

    @property
    def is_child(self) -> bool:
        return self.parent is not None

    def to_dict(self):
        return {
            'class_name': self.__class__.__name__,
            'id': self.id,
            'name': self.name,
            'status': self.status,
            'create_date': self.create_date.timestamp() if self.create_date is not None else None, 
            'start_date': self.start_date.timestamp() if self.start_date is not None else None,
            'finish_date': self.finish_date.timestamp() if self.finish_date is not None else None,
            'log': self.log,
            'progress': self.progress,
            'after': self.after,
            'childs': self.childs,
            'parent': self.parent
        }

    def start(self):
        try:
            self.runtime_status = WebQueriedWorkerStatus.Running
            self._process = multiprocessing.Process(target=self._async_main_wrapper, daemon=True)
            self._process.start()
        except Exception as e:
            self.runtime_status = WebQueriedWorkerStatus.Failed
            self.write_to_log(f'Ошибка при запуске процесса:\n{str(e)}')

    def cancel(self):
        self.runtime_status = WebQueriedWorkerStatus.Cancelled

    def finish_process(self):
        # https://docs.python.org/3/library/multiprocessing.html#all-start-methods

        if self.runtime_status in {
            WebQueriedWorkerStatus.Finished, 
            WebQueriedWorkerStatus.Failed, 
            WebQueriedWorkerStatus.PartiallyFailed,
            WebQueriedWorkerStatus.Cancelled
        }:
            # Joining processes that use queues
            while not self._log_queue.empty():
                try:
                    self._worker_log += self._log_queue.get(block=False)
                except Empty:
                    break
            
            self._log_queue.close()
            if self._process is not None:
                self._process.join()  # Joining zombie processes
                self._process.close()
            self._finished_by_pool = True

    def _async_main_wrapper(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.main())
        finally:
            loop.close()

    def _preflight_check(self, worker_pool: 'WebQueriedWorkerPool'):
        '''
        Вызывается в `WebQueriedWorkerPool.add_worker()` и `WebQueriedWorkerPool.add_workers()`, 
        передает `WebQueriedWorkerPool` как единственный аргумент.

        По задумке должен использоваться для проверки дублирующих процессов:
        ```python
        already_running_workers = [
            x for x in worker_pool.workers
            if isinstance(x, WebQueriedWorker) and x.status in ['Pending', 'Running'] 
        ]
        duplicate_workers = [x.id for x in already_running_workers if x.df_name == self.df_name]
        if duplicate_workers:
            raise WorkerCreationException('Одновременно может существовать не больше одного процесса.')

        ```
        '''
        raise NotImplementedError

    async def main(self):
        raise NotImplementedError


class WebQueriedWorkerPool:
    # Да, реализация странная, все вроде синхронное, но stop() нет. 
    # В текущем функционале меня такое в целом устраивает

    def __init__(self, max_running_workers: int = 10):
        self._workers: list[WebQueriedWorker] = []
        self.max_running_workers = max_running_workers
        self._running = False
        self._main_loop_coro = None

    def start(self):
        self._running = True
        self._main_loop_coro = asyncio.to_thread(self.main_loop)

    async def stop(self):
        self._running = False
        await self._main_loop_coro

    @property
    def workers(self) -> list[WebQueriedWorker]:
        return self._workers

    def worker_by_id(self, worker_id: str) -> WebQueriedWorker:
        try:
            return next(w for w in self._workers if w.id == worker_id)
        except StopIteration:
            raise FileNotFoundError(f"Процесс {worker_id} не найден")
    
    def workers_by_id(self, worker_ids: list[str]) -> list[WebQueriedWorker]:
        return [w for w in self._workers if w.id in worker_ids]
    
    def workers_by_class_names_and_status(self, class_names: list[str], statuses: list[str]) -> list[WebQueriedWorker]:
        return [w for w in self._workers if w.__class__.__name__ in class_names and w.status in statuses]
    
    def add_worker(self, worker: WebQueriedWorker):
        try:
            worker._preflight_check(self)
        except NotImplementedError:
            pass

        self._workers.append(worker)

    def add_workers(self, workers: list[WebQueriedWorker]):
        # если префлайт чек не выполнен хоть у одного - не добавится ни один
        for worker in workers:
            try:
                worker._preflight_check(self)
            except NotImplementedError:
                pass
        for worker in workers:
            self._workers.append(worker)
            
    
    def delete_worker(self, worker_id: str):
        worker = self.worker_by_id(worker_id)
        if worker.runtime_status in [
            WebQueriedWorkerStatus.Failed, 
            WebQueriedWorkerStatus.Cancelled, 
            WebQueriedWorkerStatus.Finished, 
            WebQueriedWorkerStatus.PartiallyFailed
        ]:
            try:
                self._workers.remove(worker)
            except ValueError:
                raise Exception(f'Процесс {worker_id} не найден')
        else:
            raise Exception('Можно удалить процесс только в статусах "Ошибка", "Отменен", "Завершен", "Есть ошибки"')
    
    def _pending_workers(self) -> list[WebQueriedWorker]:
        return [w for w in self._workers if w.status == 'Pending']
    
    def _finished_workers(self) -> list[WebQueriedWorker]:
        return [
            w for w in self._workers 
            if w.status in ['Finished', 'PartiallyFailed', 'Failed'] and not w._finished_by_pool
        ]
    
    @property
    def running_count(self) -> int:
        return len([w for w in self.workers if w.status == 'Running'])
    
    def _dependencies_check(self, worker: WebQueriedWorker) -> Literal['run', 'cancel', 'wait']:
        if worker.after:
            after_workers = self.workers_by_id(worker.after)
            if any(w.runtime_status in WORKER_BAD_STATUSES for w in after_workers):
                return 'cancel'
            
            if any(w.runtime_status != WebQueriedWorkerStatus.Finished for w in after_workers):
                return 'wait'
        return 'run'
    
    def worker_should_partially_fail(self, worker: WebQueriedWorker) -> bool:
        if worker.childs:
            child_workers = self.workers_by_id(worker.childs)
            return any(w.runtime_status in WORKER_BAD_STATUSES for w in child_workers)
        return False
    
    def main_loop(self):
        while self._running:
            sleep(1)
            
            pending_workers = self._pending_workers()
            for worker in pending_workers:
                if self.running_count >= self.max_running_workers:
                    break

                action = self._dependencies_check(worker)
                match action:
                    case 'wait':
                        continue
                    case 'cancel':
                        worker.cancel()
                        continue
                    case 'run':
                        worker.start()
                        continue
            
            finished_workers = self._finished_workers()
            for worker in finished_workers:
                worker.finish_process()

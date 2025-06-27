from enum import Enum
from hashlib import md5
from random import randbytes
from datetime import datetime
from typing import List, Optional
import asyncio


class WebQueriedWorkerStatus(Enum):
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
        worker_pool: 'WebQueriedWorkerPool',
        name: str = 'WebQueriedWorker',
        parent: Optional[str] = None,
        after: Optional[List[str]] = None,
        childs: Optional[List[str]] = None
    ):
        self.worker_pool = worker_pool
        self.name = name
        self.id = md5(randbytes(16)).hexdigest()
        self.create_date = datetime.now()
        self.start_date: Optional[datetime] = None
        self.finish_date: Optional[datetime] = None
        self._runtime_status: Optional[WebQueriedWorkerStatus] = None
        self.progress: Optional[float] = None
        self.worker_log = ''
        self._task: Optional[asyncio.Task] = None
        
        self.runtime_status = WebQueriedWorkerStatus.Pending
        
        self.parent = parent
        self.after = after or []
        self.childs = childs or []

    def to_dict(self):
        ret = {
                'class_name': self.__class__.__name__,
                'id': self.id,
                'name': self.name,
                'status': self.status,
                'create_date': str(self.create_date), 
                'start_date': str(self.start_date), 
                'finish_date': str(self.finish_date),
                'log': self.log,
                'progress': self.progress,
                'after': self.after,
                'childs': self.childs,
                'parent': self.parent
            }
        return ret

    def write_to_log(self, message: str):
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
        ret = self.worker_log
        return ret

    async def start(self):
        try:
            self.runtime_status = WebQueriedWorkerStatus.Running
            self._task = asyncio.create_task(self._run_wrapper())
        except Exception as e:
            self.runtime_status = WebQueriedWorkerStatus.Failed
            await self.write_to_log(f'Ошибка при запуске процесса:\n{str(e)}')

    async def _run_wrapper(self):
        try:
            await self.main()
            if self.runtime_status == WebQueriedWorkerStatus.Running:
                self.runtime_status = WebQueriedWorkerStatus.Finished
        except Exception as e:
            self.runtime_status = WebQueriedWorkerStatus.Failed
            await self.write_to_log(f'Необработанная ошибка:\n{str(e)}')

    async def cancel(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                self.runtime_status = WebQueriedWorkerStatus.Cancelled

    async def main(self):
        raise NotImplementedError('Для запуска процесса необходимо переопределить метод main()')


class WebQueriedWorkerPool:
    def __init__(self, max_running_workers: int = 10):
        self._workers: List[WebQueriedWorker] = []
        self.max_running_workers = max_running_workers
        self._pending_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self):
        self._running = True
        self._pending_task = asyncio.create_task(self._start_pending())

    async def stop(self):
        """Корректная остановка пула"""
        self._running = False
        if self._pending_task:
            await self._pending_task

    @property
    def workers(self) -> List[WebQueriedWorker]:
        return self._workers

    def worker_by_id(self, worker_id: str) -> WebQueriedWorker:
        try:
            return next(w for w in self._workers if w.id == worker_id)
        except StopIteration:
            raise FileNotFoundError(f"Worker {worker_id} not found")
        
    def workers_by_id(self, worker_ids: List[str]) -> List[WebQueriedWorker]:
        return [w for w in self._workers if w.id in worker_ids]
        
    def add_worker(self, worker: WebQueriedWorker):
        self._workers.append(worker)
    
    def add_workers(self, workers: List[WebQueriedWorker]):
        self._workers.extend(workers)

    def delete_worker(self, worker_id: str):
        worker = self.worker_by_id(worker_id)
        if worker.runtime_status in [
            WebQueriedWorkerStatus.Failed, 
            WebQueriedWorkerStatus.Cancelled, 
            WebQueriedWorkerStatus.Finished, 
            WebQueriedWorkerStatus.PartiallyFailed
        ]:
            self._workers.remove(worker)
        else:
            raise Exception('Можно удалить процесс только в статусах "Ошибка", "Отменен", "Завершен", "Есть ошибки"')
    
    def _pending_workers(self) -> List[WebQueriedWorker]:
        return [w for w in self.workers if w.status == 'Pending']
    
    @property
    def running_count(self) -> int:
        return len([w for w in self.workers if w.status == 'Running'])

    async def _check_worker_dependencies_before_start(self, worker: WebQueriedWorker) -> str:
        if worker.after:
            after_workers = self.workers_by_id(worker.after)
            if any(w.runtime_status in WORKER_BAD_STATUSES for w in after_workers):
                return 'cancel'
            
            if any(w.runtime_status != WebQueriedWorkerStatus.Finished for w in after_workers):
                return 'wait'
        return 'run'
    
    async def worker_should_partially_fail(self, worker: WebQueriedWorker) -> bool:
        if worker.childs:
            child_workers = self.workers_by_id(worker.childs)
            return any(w.runtime_status in WORKER_BAD_STATUSES for w in child_workers)
        return False

    async def _start_pending(self):
        while self._running:
            await asyncio.sleep(0.1)
            
            pending_workers = self._pending_workers()
            for worker in pending_workers:
                if self.running_count >= self.max_running_workers:
                    break

                action = await self._check_worker_dependencies_before_start(worker)
                match action:
                    case 'wait':
                        continue
                    case 'cancel':
                        await worker.cancel()
                        continue
                    case 'run':
                        await worker.start()
                        continue
                    case _:
                        continue

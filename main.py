import logging
import multiprocessing
import os
import random
import signal
import time
from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum
from logging import handlers
from multiprocessing import Queue, Process
from queue import Empty

logging.basicConfig(level=logging.INFO)
STOP = False
READ_TIMEOUT = 5
CHECK_QUEUES_TIMEOUT = 15


def signal_handler(sig, frame):
    """
    Перехватываем завершение и делаем graceful shutdown
    """

    global STOP
    if STOP:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        os.kill(os.getpid(), signal.SIGTERM)
    STOP = True


signal.signal(signal.SIGINT, signal_handler)


class CarBrokenError(Exception):
    pass


class IncorrectAddressError(Exception):
    pass


class PackingType(IntEnum):
    ENVELOPE = 0
    BOX = 1
    TUBE = 2
    ROLL = 3


@dataclass
class Package:
    # Unique identifier of a package
    idx: int
    # Length, width, height in cm
    dimensions: tuple[int, int, int]
    # Weight in kg
    weight: float
    type: PackingType


class PickupBox:
    """
    Represents a box of packages to deliver
    """
    max_packages = 64

    def __init__(self, packages: list[Package]):
        if len(packages) > self.max_packages:
            raise ValueError(f"Too many packages for pick up: {len(packages)}")

        self.__packages = packages

    def __len__(self) -> int:
        return len(self.__packages)

    @property
    def packages(self) -> list[Package]:
        return self.__packages


class DeliveryGenerator:
    """
    Generates new packages for delivery
    Неправильно реализованный генератор https://docs.python.org/3/library/stdtypes.html#generator-types
    """


    def __init__(self):
        self.idx = 0
        self.__gen = self.__generate()

    def __iter__(self) -> "DeliveryGenerator":
        return self

    def __next__(self):
        return next(self.__gen)

    def __generate(self) -> Package:
        while True:
            package_type = random.choice(list(PackingType))
            dimensions = (
                random.randint(0, 100),
                random.randint(0, 100),
                random.randint(0, 10)
            )
            weight = random.random() * 100
            package = Package(idx=self.idx, dimensions=dimensions, weight=weight, type=package_type)
            self.idx += 1

            yield package


class PickUpCar:
    """
    A car can only pick up a box of packages and the driver will deliver them one by one
    """

    def __init__(
            self,
            new_delivery_queue: Queue,
            return_packages_queue: Queue,
            no_address_packages_queue: Queue,
            redelivered: multiprocessing.Value,
            delivered: multiprocessing.Value,
            rlock: multiprocessing.RLock,
            logs_queue: Queue,
    ):
        self.new_delivery_queue = new_delivery_queue
        self.return_packages_queue = return_packages_queue
        self.no_address_packages_queue = no_address_packages_queue
        self.delivered = delivered
        self.redelivered = redelivered
        self.rlock = rlock

        qh = handlers.QueueHandler(logs_queue)
        self.root = logging.getLogger()
        self.root.setLevel(logging.INFO)
        self.root.addHandler(qh)

    def handle_delivery(self) -> None:
        while True:
            try:
                box = self.new_delivery_queue.get(timeout=READ_TIMEOUT)
            except Empty:
                return
            self.deliver_box(box)

    def deliver_box(self, pickup_box: PickupBox) -> None:
        """
        Deliver box of packages
        :param pickup_box: box of packages to deliver
        :return: None
        """
        for package in pickup_box.packages:
            try:
                self.__deliver_package(package)

            except IncorrectAddressError:
                self.__send_to_lost_and_found(package)
            except CarBrokenError:
                self.__return_packages_to_warehouse(pickup_box.packages)
            else:
                pickup_box.packages.remove(package)

    def __deliver_package(self, package: Package) -> Package:
        """
        Deliver one package
        :param package: package to deliver
        :return: package if the delivery was successful
        :raises:
            - CarBrokenError
            - IncorrectAddressError
        """
        # Небезопасный рандом, берет timestamp из системы, выставляем сид для каждого процесса
        random.seed(str(os.getpid()) + str(time.time()))
        rnd = random.random()
        match rnd:
            case rnd if rnd > 0.99:
                raise CarBrokenError()
            case rnd if 0.98 < rnd <= 0.99:
                raise IncorrectAddressError()
            case _:
                time.sleep(0.01)
                self.root.info(f"{datetime.now()}: Package idx={package.idx} delivered!")
                with self.rlock:
                    self.delivered.value = self.delivered.value + 1
                return package

    def __send_to_lost_and_found(self, package) -> None:
        """
        Send package to lost and found
        :param package: package to return
        :return: None
        """
        self.no_address_packages_queue.put(package)

    def __return_packages_to_warehouse(self, packages) -> None:
        """
        Send packages back to warehouse
        :param packages: packages to return
        :return: None
        """
        self.root.error(f"{datetime.now()}: Returning {len(packages)} packages")
        for package in packages:
            with self.rlock:
                self.redelivered.value = self.redelivered.value + 1
            self.return_packages_queue.put(package)


class WarehouseDispatcher:
    """
    Class that manages all work in the warehouse
    """

    def __init__(
            self,
            delivery_queue: Queue,
            packages_queue: Queue,
            no_address_packages_queue: Queue,
            lost,
            rlock,
            logs_queue,
    ):
        self.delivery_queue = delivery_queue
        self.packages_queue = packages_queue
        self.no_address_packages_queue = no_address_packages_queue
        self.lost = lost
        self.rlock = rlock

        qh = handlers.QueueHandler(logs_queue)
        self.root = logging.getLogger()
        self.root.setLevel(logging.INFO)
        self.root.addHandler(qh)

    def handle_incoming_packages(self) -> None:
        """
        Groups all incoming packages into boxes and send for delivery,
        When the packages run out, it will send unfinished box
        :return: None
        """

        pallet = PickupBox(packages=[])
        while True:
            try:
                package = self.packages_queue.get(timeout=READ_TIMEOUT)
            except Empty:
                self.delivery_queue.put(pallet)
                return

            pallet.packages.append(package)
            if len(pallet) == pallet.max_packages:
                self.delivery_queue.put(pallet)
                pallet = PickupBox([])

    def handle_lost_packages(self) -> None:
        """
        Send all packages without address into the middle of nowhere
        :return: None
        """
        while True:
            try:
                package = self.no_address_packages_queue.get(timeout=READ_TIMEOUT)
            except Empty:
                return

            with self.rlock:
                self.lost.value = self.lost.value + 1
            self.root.error(f"{datetime.now()} На деревню дедушке! {package}")


def do_stuff(packages_queue: Queue) -> None:
    packages_generator = DeliveryGenerator()
    cnt = 0
    for package in packages_generator:
        packages_queue.put(package)
        cnt += 1
        if STOP or cnt == 1000:
            return


def logger_process(logging_queue: Queue) -> None:
    """
    Отдельный процесс для логирования, что бы избежать гонок
    https://docs.python.org/3/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes
    """

    while True:
        try:
            record = logging_queue.get(timeout=READ_TIMEOUT)
        except Empty:
            return
        logger = logging.getLogger(record.name)
        logger.handle(record)


if __name__ == '__main__':
    """
    Запускаем менеджер управления процессами, что бы он управлял ресурсами
    http://onreader.mdl.ru/MasteringConcurrencyInPython/content/Ch06.html
    http://onreader.mdl.ru/CPythonInternals/content/Ch10.html#0309
    """

    with multiprocessing.Manager() as pool_manager:
        # Объявлем примитивы синхронизации процессов
        rlock = pool_manager.RLock()

        # Очереди
        incoming_packages_queue = pool_manager.Queue()
        ready_to_deliver_queue = pool_manager.Queue()
        return_of_bad_packages_queue = pool_manager.Queue()
        logging_queue = pool_manager.Queue()

        # Собираем статистику
        delivered = pool_manager.Value('i', 0)
        lost = pool_manager.Value('i', 0)
        redelivered = pool_manager.Value('i', 0)

        processes = [
            Process(
                target=PickUpCar(
                    ready_to_deliver_queue,
                    incoming_packages_queue,
                    return_of_bad_packages_queue,
                    redelivered,
                    delivered,
                    rlock,
                    logging_queue,
                ).handle_delivery,
                daemon=True,
            ) for _ in range(os.cpu_count())
        ]

        dispatcher = WarehouseDispatcher(
            ready_to_deliver_queue,
            incoming_packages_queue,
            return_of_bad_packages_queue,
            lost,
            rlock,
            logging_queue,
        )

        processes.append(Process(target=dispatcher.handle_incoming_packages, daemon=True))
        processes.append(Process(target=dispatcher.handle_lost_packages, daemon=True))
        processes.append(Process(target=do_stuff, args=(incoming_packages_queue,), daemon=True))
        processes.append(Process(target=logger_process, args=(logging_queue,), daemon=True))

        for process in processes:
            process.start()

        try:
            while True:
                """
                Проверяем что очереди пустые - перестали работать. 
                Ждем некоторое время что бы все процессы успели завершиться
                """
                time.sleep(CHECK_QUEUES_TIMEOUT)
                if (
                        incoming_packages_queue.empty()
                        and ready_to_deliver_queue.empty()
                        and return_of_bad_packages_queue.empty()
                ):
                    time.sleep(CHECK_QUEUES_TIMEOUT)
                    break

        finally:
            print("DELIVERED", delivered.value)
            print("REDELIVERED", redelivered.value)
            print("LOST", lost.value)

    [process.terminate() for process in processes]

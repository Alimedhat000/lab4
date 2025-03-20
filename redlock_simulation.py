import redis
import time
import uuid
import multiprocessing
import random
import logging
import os
from datetime import datetime

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Generate a log filename with timestamp
log_filename = f"logs/redlock_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Set up logging to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

client_processes_waiting = [0, 1, 1, 1, 4]

DEFAULT_RETRY_TIMES = 3
DEFAULT_RETRY_DELAY = 200
DEFAULT_TTL = 100000
CLOCK_DRIFT_FACTOR = 0.01

class Redlock:
    def __init__(self, redis_nodes):
        """
        Initialize Redlock with a list of Redis node addresses.
        :param redis_nodes: List of (host, port) tuples.
        """
        self.redis_nodes = redis_nodes
        self.redis_clients = []
        self.resource = None
        self.__lock_id = None
        self.quorum = len(self.redis_nodes) // 2 + 1
        
        for host, port in self.redis_nodes:
            try:
                client = redis.Redis(host=host, port=port)
                self.redis_clients.append(client)
                logger.info(f"Connected to Redis node {host}:{port}")
            except Exception as e:
                logger.error(f"Failed to connect to Redis node {host}:{port}  : {e}")

    def acquire_lock(self, resource, ttl):
        """
        Try to acquire a distributed lock.
        :param resource: The name of the resource to lock.
        :param ttl: Time-to-live for the lock in milliseconds.
        :return: Tuple (lock_acquired, lock_id).
        """
        self.__lock_id = uuid.uuid4().hex
        self.resource = resource

        for retry in range(DEFAULT_RETRY_TIMES + 1):
            acquired_node_count = 0
            start_time = time.monotonic()

            try:
                for node in self.redis_nodes:
                    if self.acquire_node(node):
                        acquired_node_count += 1
                
                end_time = time.monotonic()
                elapsed_milliseconds = (end_time - start_time) * 10**3

                drift = (ttl * CLOCK_DRIFT_FACTOR) + 2
                validity = ttl - (elapsed_milliseconds + drift)
                
                if acquired_node_count >= self.quorum and validity > 0:
                    logger.info(f"Lock acquired on resource '{resource}' with ID {self.__lock_id}")
                    return True, self.__lock_id
                else:
                    logger.warning(f"Failed to acquire lock on resource '{resource}', releasing partial locks")
                    for node in self.redis_nodes:
                        self.release_node(node)
                    time.sleep(random.randint(0, DEFAULT_RETRY_DELAY) / 1000)
            except Exception as e:
                
                logger.error(f"Error acquiring lock on resource '{resource}': {e}")
        
        logger.warning(f"Lock acquisition failed after retries for resource '{resource}'")
        return False, self.__lock_id

    def release_lock(self, resource, lock_id):
        """
        Release the distributed lock.
        :param resource: The name of the resource to unlock.
        :param lock_id: The unique lock ID to verify ownership.
        """
        pass
    
    def acquire_node(self, node):
        """
        acquire a single redis node
        """
        try:
            return node.set(self.resource, self.__lock_id, nx=True, px=DEFAULT_TTL)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            logger.error(f"Error acquiring lock on node {node}: {e}")
            return False

    def release_node(self, node):
        """
        release a single redis node
        """
        try:
            node._release_script(keys=[self.resource], args=[self.__lock_id])
            logger.info(f"Lock released on node {node}")
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            logger.error(f"Error releasing lock on node {node}: {e}")


def client_process(redis_nodes, resource, ttl, client_id):
    """
    Function to simulate a single client process trying to acquire and release a lock.
    """
    time.sleep(client_processes_waiting[client_id])

    redlock = Redlock(redis_nodes)
    logger.info(f"Client-{client_id}: Attempting to acquire lock...")
    lock_acquired, lock_id = redlock.acquire_lock(resource, ttl)

    if lock_acquired:
        logger.info(f"Client-{client_id}: Lock acquired! Lock ID: {lock_id}")
        time.sleep(3)  # Simulate some work
        redlock.release_lock(resource, lock_id)
        logger.info(f"Client-{client_id}: Lock released!")
    else:
        logger.warning(f"Client-{client_id}: Failed to acquire lock.")

if __name__ == "__main__":
    redis_nodes = [
        ("localhost", 63791),
        ("localhost", 63792),
        ("localhost", 63793),
        ("localhost", 63794),
        ("localhost", 63795),
    ]

    resource = "shared_resource"
    ttl = 5000  # Lock TTL in milliseconds (5 seconds)

    num_clients = 5

    processes = []
    for i in range(num_clients):
        process = multiprocessing.Process(target=client_process, args=(redis_nodes, resource, ttl, i))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

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

DEFAULT_RETRY_TIMES = 10
DEFAULT_RETRY_DELAY = 200
DEFAULT_TTL = 100000
CLOCK_DRIFT_FACTOR = 0.01

"""
Redlock Algorithm Implementation:

    1. Initialize a list of Redis nodes.
    2. Get current time in milliseconds (to measure elapsed time).
    3. Try to acquire the lock on at least N/2 + 1 instances:
    4. For each node:
        - Generate a unique lock ID.
        - Attempt to set the lock with a TTL.
        - If successful, increment the count of acquired locks.
    5. If the count of acquired locks is greater than or equal to the quorum:
        - Calculate the elapsed time.
        - If the elapsed time is less than the TTL, consider the lock valid.
    6. If the lock is valid, return success.
    7. If the lock is not valid, release any acquired locks and retry.
    8. If the lock is acquired, perform the critical section.
    9. After the critical section, release the lock from all nodes.
    10. Handle errors and retries appropriately.
    
"""
class Redlock:
    def __init__(self, redis_nodes):
        """
        Initialize Redlock with a list of Redis node addresses.
        :param redis_nodes: List of (host, port) tuples.
        """
        self.redis_nodes=redis_nodes
        self.redis_clients = []
        self.resource=None
        self.__lock_id=None
        self.retry_times = 3
        self.quorum = len(self.redis_nodes) // 2 + 1
        
        self.unlock_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        # Initialize Redis nodes
        for host, port in self.redis_nodes:
            try:
                client = redis.Redis(host=host,port=port)
                self.redis_clients.append(client)
                logger.info(f"Connected to Redis node {host}:{port}")
            except Exception as e:
                logger.error(f"Failed to connect to Redis node {host}:{port}  : {e}")

    def acquire_lock(self, resource, client_index, ttl):
        """
        Try to acquire a distributed lock.
        :param resource: The name of the resource to lock.
        :param ttl: Time-to-live for the lock in milliseconds.
        :return: Tuple (lock_acquired, lock_id).
        """
        # lock_key should be random and unique for each resource request
        self.__lock_id = uuid.uuid4().hex
        self.resource = resource

        for retry in range(self.retry_times + 1):
            acquired_node_count = 0
            start_time = time.monotonic()

            try:
                for i, node in enumerate(self.redis_clients):
                    if self.acquire_node(node, i,ttl):
                        acquired_node_count += 1
                
                end_time = time.monotonic()
                elapsed_milliseconds = (end_time - start_time) * 10**3


                # Add 2 milliseconds to the drift to account for Redis expires
                # precision, which is 1 milliescond, plus 1 millisecond min drift
                # for small TTLs.
                drift = (ttl * CLOCK_DRIFT_FACTOR) + 2

                validity = ttl - (elapsed_milliseconds + drift)
                
                # If count > majority and elapsed time > TTL, lock is valid 
                if acquired_node_count >= self.quorum and validity > 0:
                        logger.info(f"Client-{client_index}: Lock acquired on resource '{resource}' with ID {self.__lock_id}")
                        return True, self.__lock_id
                else:
                    logger.warning(f"Client-{client_index}: Failed to acquire lock on resource '{resource}', releasing partial locks")
                    for i, node in enumerate(self.redis_clients):
                        self.release_node(node, i)
                    time.sleep(random.randint(0, DEFAULT_RETRY_DELAY) / 1000)
                    
            except Exception as e:
                    
                    logger.error(f"Client-{client_index}: Error acquiring lock on resource '{resource}': {e}")
        
        logger.warning(f"Client-{client_index}: Lock acquisition failed after retries for resource '{resource}'")
        return False, self.__lock_id

    def release_lock(self, resource, lock_id):
        """
        Release the distributed lock.
        :param resource: The name of the resource to unlock.
        :param lock_id: The unique lock ID to verify ownership.
        """
        if not lock_id:
            return
        
        for i, client in enumerate(self.redis_clients):
            try:
                self.release_node(client, i + 1)
                logger.info(f"Lock '{resource} with ID {lock_id} released on node {i + 1}")
            except Exception as e:
                logger.error(f"Failed to release lock on node {i + 1}")
    
    def acquire_node(self, node, index,ttl):
        """
        acquire a single redis node
        """
        try:
            return node.set(self.resource, self.__lock_id, nx=True, px=ttl)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            logger.error(f"Error acquiring lock on node {index}: {e}")
            return False

    def release_node(self, node, index):
        """
        release a single redis node
        """
        try:
            # The Lua script checks if the lock ID matches before deleting
            script = node.register_script(self.unlock_script)
            script(keys=[self.resource], args=[self.__lock_id])
            logger.info(f"Lock released on node {index}")
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            logger.error(f"Error releasing lock on node {index}: {e}")


def client_process(redis_nodes, resource, ttl, client_id):
    
    """
    Function to simulate a single client process trying to acquire and release a lock.
    """
    time.sleep(client_processes_waiting[client_id])

    redlock = Redlock(redis_nodes)
    logger.info(f"Client-{client_id}: Attempting to acquire lock...")
    # Try to acquire the lock
    lock_acquired, lock_id = redlock.acquire_lock(resource, client_id, ttl)

    if lock_acquired:
        logger.info(f"Client-{client_id}: Lock acquired! Lock ID: {lock_id}")
        
        # Simulate critical section
        critical_section(client_id)
        
        redlock.release_lock(resource, lock_id)
        logger.info(f"Client-{client_id}: Lock released!")
        logger.info(f"Client-{client_id}: Lock released!")
    else:
        logger.warning(f"Client-{client_id}: Failed to acquire lock.")

def critical_section(client_id):
    """
    Simulates critical section by writing to a shared file.
    """
    greetings = ["Hi", "Hello", "Hey", "Greetings"]
    greeting = random.choice(greetings)
    
    with open("shared_file.txt", "a") as f:
        message = f"Client-{client_id}: {greeting}!\n"
        f.write(message)
        logger.info(f"Client-{client_id} wrote to file: {message.strip()}")

if __name__ == "__main__":
    # Define Redis node addresses (host, port)
    redis_nodes = [
        ("localhost", 63791),
        ("localhost", 63792),
        ("localhost", 63793),
        ("localhost", 63794),
        ("localhost", 63795),
    ]

    resource = "shared_resource"
    ttl = 5000  # Lock TTL in milliseconds (5 seconds)

    # Number of client processes
    num_clients = 5

    # Start multiple client processes
    processes = []
    for i in range(num_clients):
        process = multiprocessing.Process(target=client_process, args=(redis_nodes, resource, ttl, i))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

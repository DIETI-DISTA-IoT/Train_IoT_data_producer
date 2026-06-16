import pandas as pd
import numpy as np
import time
from scipy.stats import lognorm
import pickle
import threading
import time
from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient
from confluent_kafka.serialization import StringSerializer
import logging
import json
import argparse
import signal
import os
import requests
import subprocess
import atexit
from flask import Flask
import socket
from OpenFAIR.container_api import ContainerAPI
from OpenFAIR import Train, EventType
BASE_DIR = os.path.dirname(__file__)


# Create a lock object
lock = threading.Lock()

def synchronized(lock):
    """ Synchronization decorator. """
    def wrapper(f):
        def wrapped(*args, **kwargs):
            with lock:
                return f(*args, **kwargs)
        return wrapped
    return wrapper


# load the copula objects later:
with open(os.path.join(BASE_DIR, 'copula_anomalie.pkl'), 'rb') as f:
    copula_anomalie = pickle.load(f)

with open(os.path.join(BASE_DIR, 'copula_normali.pkl'), 'rb') as f:
    copula_normali = pickle.load(f)

produced_records = 0
produced_attacks = 0
produced_anomalies = 0
produced_diagnostics = 0
stop_threads = False
anomaly_generators = {}
diagnostics_generators = {}

virtual_train = None
eval_virtual_train = None

HOST_IP = os.getenv("HOST_IP")

# load the probabilities of the classes:
anomaly_probabilities = pd.read_csv(os.path.join(BASE_DIR, 'generators', 'anomaly_cluster_probabilities.csv'))
diagnostics_probabilities = pd.read_csv(os.path.join(BASE_DIR, 'generators', 'diagnostics_cluster_probabilities.csv'))

# Constants:
columns_to_generate = [
    'Durata', 'CabEnabled_M1', 'CabEnabled_M8', 'ERTMS_PiastraSts', 'HMI_ACPntSts_T2', 'HMI_ACPntSts_T7',
    'HMI_DCPntSts_T2', 'HMI_DCPntSts_T7', 'HMI_Iline', 'HMI_Irsts_T2', 'HMI_Irsts_T7', 'HMI_VBatt_T2',
    'HMI_VBatt_T4', 'HMI_VBatt_T5', 'HMI_VBatt_T7', 'HMI_Vline', 'HMI_impSIL', 'LineVoltType',
    'MDS_StatoMarcia', '_GPS_LAT', '_GPS_LON', 'ldvvelimps', 'ldvveltreno', 'usB1BCilPres_M1', 'usB1BCilPres_M3',
    'usB1BCilPres_M6', 'usB1BCilPres_M8', 'usB1BCilPres_T2', 'usB1BCilPres_T4', 'usB1BCilPres_T5', 'usB1BCilPres_T7',
    'usB2BCilPres_M1', 'usB2BCilPres_M3', 'usB2BCilPres_M6', 'usB2BCilPres_M8', 'usB2BCilPres_T2', 'usB2BCilPres_T4',
    'usB2BCilPres_T5', 'usB2BCilPres_T7', 'usBpPres', 'usMpPres'
]

all_columns = [
    'Flotta', 'Veicolo', 'Codice', 'Nome', 'Descrizione', 'Timestamp', 'Timestamp chiusura', 'Durata', 
    'Posizione', 'Sistema', 'Componente', 'Timestamp segnale'
] + columns_to_generate


def parse_str_list(arg):
    # Split the input string by commas and convert each element to int
    try:
        return [str(x) for x in arg.split(',')]
    except ValueError:
        raise argparse.ArgumentTypeError("Arguments must be strings separated by commas")
    

def parse_int_list(arg):
    # Split the input string by commas and convert each element to int
    try:
        return [int(x) for x in arg.split(',')]
    except ValueError:
        raise argparse.ArgumentTypeError("Arguments must be integers separated by commas")


@synchronized(lock)
def produce_message(data, topic_name):
    global produced_records
    """
    Produce a message to Kafka for a specific sensor type.

    Args:
        data (dict): The data to be sent as a message.
        topic_name (str): The Kafka topic to which the message will be sent.
    """
    try:
        producer.produce(topic=topic_name, value=data)  # Send the message to Kafka
        if topic_name.endswith('HEALTH'):
            pass
        else:
            produced_records += 1

        if produced_records % 50 == 0:
            producer.flush()
        if produced_records % 100 == 0:
            logger.info(f"sent {produced_records} records for now. {produced_attacks} attacks, {produced_anomalies} anomalies, and {produced_diagnostics} diagnostics.")
    except Exception as e:
        print(f"Error while producing message to {topic_name} : {e}")


def sample_anomaly_from_global():
    return -1, copula_anomalie.sample(1)


def sample_anomaly_from_clusters():
    cluster_to_sample_from = np.random.choice(
        anomaly_probabilities['cluster'],
        size=1, 
        p=anomaly_probabilities['probability'])
    return cluster_to_sample_from[0], anomaly_generators[cluster_to_sample_from[0]].sample(1)


def health_probes_thread(args):
    logger.info(f"Starting thread for health probes for vehicle: {VEHICLE_NAME}")
    while not stop_threads:
        health_dict = train_monitor.probe_health()
        produce_message(
            data=health_dict, 
            topic_name=f"{VEHICLE_NAME}_HEALTH")
        time.sleep(args.probe_frequency_seconds)


def round_dict_numbers(d, n):
    """Round all numeric values in a dictionary to n decimal places."""
    return {
        key: round(value, n) if isinstance(value, (int, float)) else value
        for key, value in d.items()
    }


def convert_dict_to_json_serializable(d):
    """Convert numpy types to native Python types."""
    return {
        key: int(value) if isinstance(value, np.integer) else 
             float(value) if isinstance(value, np.floating) else 
             value
        for key, value in d.items()
    }


def thread_anomalie(args):
    global produced_anomalies, attack_lock, virtual_train, eval_virtual_train, produced_attacks
    logger.info(f"Starting thread for anomalies generation for vehicle: {VEHICLE_NAME}")
    media_durata_anomalie = args.mu_anomalies * args.alpha
    sigma_anomalie = 1 * args.beta
    lognormal_anomalie = lognorm(s=sigma_anomalie, scale=np.exp(np.log(media_durata_anomalie)))
    topic_name = f"{VEHICLE_NAME}_anomalies"
    eval_topic_name = f"{VEHICLE_NAME}_eval_anomalies"

    while not stop_threads:

        with attack_lock:

            if get_status_robust() == 'INFECTED':
                event = EventType.ATTACK
                produced_attacks += 1
            else:
                event = EventType.ANOMALY
                produced_anomalies += 1

        synthetic_anomaly = virtual_train.step(event)
        eval_synth_anomaly = eval_virtual_train.step(event, adversarial=True)

        durata_anomalia = lognormal_anomalie.rvs(size=1)
        synthetic_anomaly['Durata'] = durata_anomalia[0]
        eval_synth_anomaly['Durata'] = durata_anomalia[0]
        synthetic_anomaly['Flotta'] = 'ETR700'
        eval_synth_anomaly['Flotta'] = 'ETR700'
        synthetic_anomaly['Veicolo'] = VEHICLE_NAME
        eval_synth_anomaly['Veicolo'] = VEHICLE_NAME
        synthetic_anomaly['Timestamp'] = pd.Timestamp.now()
        eval_synth_anomaly['Timestamp'] = synthetic_anomaly['Timestamp']
        synthetic_anomaly['Timestamp chiusura'] = pd.to_datetime(synthetic_anomaly['Timestamp'] + pd.to_timedelta(synthetic_anomaly['Durata'], unit='s'))
        eval_synth_anomaly['Timestamp chiusura'] = synthetic_anomaly['Timestamp chiusura']
       
                
        synthetic_anomaly = round_dict_numbers(synthetic_anomaly,4)
        eval_synth_anomaly = round_dict_numbers(eval_synth_anomaly,4)

        data_to_send = convert_dict_to_json_serializable(synthetic_anomaly)
        data_to_send['Timestamp'] = str(data_to_send['Timestamp'])
        data_to_send['Timestamp chiusura'] = str(data_to_send['Timestamp chiusura'])

        eval_data_to_send = convert_dict_to_json_serializable(eval_synth_anomaly)
        eval_data_to_send['Timestamp'] = str(eval_data_to_send['Timestamp'])
        eval_data_to_send['Timestamp chiusura'] = str(eval_data_to_send['Timestamp chiusura'])

        produce_message(data_to_send, topic_name)
        produce_message(eval_data_to_send, eval_topic_name)
        if args.time_emulation:
            time.sleep(durata_anomalia[0])


def sample_normal_from_global():
    return -1, copula_normali.sample(1)


def sample_normal_from_clusters():
    cluster_to_sample_from = np.random.choice(
        diagnostics_probabilities['cluster'],
        size=1, 
        p=diagnostics_probabilities['probability'])
    return cluster_to_sample_from[0], diagnostics_generators[cluster_to_sample_from[0]].sample(1)


def thread_normali(args):
    global produced_diagnostics, virtual_train
    logger.info(f"Starting thread for normal data generation for vehicle: {VEHICLE_NAME}")
    media_durata_normali = args.mu_normal * args.alpha
    sigma_normali = 1 * args.beta
    lognormal_normali = lognorm(s=sigma_normali, scale=np.exp(np.log(media_durata_normali)))
    topic_name = f"{VEHICLE_NAME}_normal_data"

    while not stop_threads:
        synthetic_normal = virtual_train.step(EventType.NORMAL)
        _ = eval_virtual_train.step(EventType.NORMAL, adversarial=True)
        
        durata_normale = lognormal_normali.rvs(size=1)
        synthetic_normal['Durata'] = durata_normale[0]
        synthetic_normal['Flotta'] = 'ETR700'
        synthetic_normal['Veicolo'] = VEHICLE_NAME
        synthetic_normal['Test'] = 'N'
        synthetic_normal['Timestamp'] = pd.Timestamp.now()
        synthetic_normal['Timestamp chiusura'] = synthetic_normal['Timestamp'] + pd.to_timedelta(synthetic_normal['Durata'], unit='s')
        synthetic_normal['Posizione'] = np.nan
        synthetic_normal['Sistema'] = 'VEHICLE'
        synthetic_normal['Componente'] = 'VEHICLE'
        synthetic_normal['Timestamp segnale'] = np.nan

        for col in all_columns:
            if col not in synthetic_normal.keys():
                synthetic_normal[col] = np.nan

        synthetic_normal = round_dict_numbers(synthetic_normal, 4)
        
        data_to_send = convert_dict_to_json_serializable(synthetic_normal)
        data_to_send['Timestamp'] = str(data_to_send['Timestamp'])
        data_to_send['Timestamp chiusura'] = str(data_to_send['Timestamp chiusura'])
        
        produce_message(data_to_send, topic_name)
        produced_diagnostics += 1
        if args.time_emulation:
            time.sleep(durata_normale[0])


def get_status_robust():    
    return 'INFECTED' if UNDER_ATTACK else 'HEALTHY'


def signal_handler(sig, frame):
    global stop_threads
    logger.debug(f"Received signal {sig}. Gracefully stopping {VEHICLE_NAME} producer.")
    stop_threads = True


def configure_no_proxy():
    os.environ['no_proxy'] = os.environ.get('no_proxy', '') + f",{HOST_IP}"


def cleanup_kafka():
    """Flush the Kafka producer and delete all topics owned by this producer container.
    Called on explicit stop and registered with atexit so it also runs on container shutdown."""
    global producer, admin_client
    _logger = logging.getLogger(f'[{VEHICLE_NAME}_PROD]') if 'logger' not in globals() else logger

    if producer is not None:
        try:
            producer.flush(10)
            _logger.info(f"Kafka producer flushed for {VEHICLE_NAME}")
        except Exception as e:
            _logger.warning(f"Producer flush failed (Kafka may be down): {e}")

    if admin_client is not None:
        owned_topics = [
            f"{VEHICLE_NAME}_anomalies",
            f"{VEHICLE_NAME}_eval_anomalies",
            f"{VEHICLE_NAME}_normal_data",
            f"{VEHICLE_NAME}_HEALTH",
        ]
        try:
            futures = admin_client.delete_topics(owned_topics, operation_timeout=10)
            for topic, future in futures.items():
                try:
                    future.result()
                    _logger.info(f"Deleted Kafka topic: {topic}")
                except Exception as e:
                    _logger.warning(f"Could not delete topic {topic} (may not exist or Kafka down): {e}")
        except Exception as e:
            _logger.warning(f"Topic deletion failed (Kafka may be down): {e}")


# Global state for API management
api_config = {}
api_running = False
api_threads = []
api_lock = threading.Lock()

def load_config_from_environment():
    """Load configuration from environment variables"""
    config = {
        'vehicle_name': os.getenv('VEHICLE_NAME'),
        'kafka_broker': os.getenv('KAFKA_BROKER', 'kafka:9092'),
        'logging_level': os.getenv('LOGGING_LEVEL', 'INFO'),
        'bot_port': os.getenv('BOT_PORT', '5002')
    }
    
    # Validate required environment variables
    if not config['vehicle_name']:
        raise ValueError("VEHICLE_NAME environment variable must be set")
    
    return config



def validate_config(config):
    """Validate configuration parameters"""
    # Check required fields
    required_fields = ['vehicle_name', 'kafka_broker']
    for field in required_fields:
        if not config.get(field):
            raise ValueError(f"Missing required configuration field: {field}")
    
    return True

def start_producer_threads(config):
    """Start producer threads with configuration"""
    global api_threads, api_running, anomaly_generators, diagnostics_generators, virtual_train, eval_virtual_train
    global produced_records, produced_attacks, produced_anomalies, produced_diagnostics

    with api_lock:
        if api_running:
            return False, "Producer is already running"

        # Reset all per-run counters so each run (within a reused container)
        # starts from a clean slate, matching the fresh W&B run's step 0.
        produced_records = 0
        produced_attacks = 0
        produced_anomalies = 0
        produced_diagnostics = 0

        seed = config.get('seed', None)
        ns_main = argparse.Namespace(**config)
        ns_eval = argparse.Namespace(**config)
        # Offset the eval train's seed by 1 so the two RNG streams are independent
        # while both remaining fully deterministic given the same run seed.
        if seed is not None:
            ns_eval.seed = seed + 1

        virtual_train = Train(ns_main)
        eval_virtual_train = Train(ns_eval)

        # Decouple the adversarial eval-stream noise from the live-stream knob.
        # The eval stream (published to {vehicle}_eval_* and consumed into the
        # adversarial-training buffers) uses eval_Mp_std/eval_Bp_std when set,
        # otherwise it falls back to Mp_std/Bp_std (backward-compatible).
        eval_mp = config.get('eval_Mp_std', None)
        eval_bp = config.get('eval_Bp_std', None)
        if eval_mp is not None:
            eval_virtual_train.Mp_std = eval_mp
        if eval_bp is not None:
            eval_virtual_train.Bp_std = eval_bp

        # Start threads
        anomaly_thread = threading.Thread(target=thread_anomalie, args=(argparse.Namespace(**config),))
        diagnostics_thread = threading.Thread(target=thread_normali, args=(argparse.Namespace(**config),))
        
        anomaly_thread.daemon = True
        diagnostics_thread.daemon = True
        
        anomaly_thread.start()
        diagnostics_thread.start()
        
        api_threads = [anomaly_thread, diagnostics_thread]
        api_running = True
        
        return True, "Producer started successfully"

def stop_producer_threads():
    """Stop all producer threads"""
    global stop_threads, api_threads, api_running
    
    with api_lock:
        if not api_running:
            return False, "Producer is not running"
        
        stop_threads = True
        
        for thread in api_threads:
            logger.info(f"joining to thread: {thread}. Please wait...")
            thread.join(timeout=5)
            logger.info(f"Joined to thread!")
        api_threads = []
        api_running = False

        cleanup_kafka()
        stop_threads = False  # Reset for next start
        
        return True, "Producer stopped successfully"

class ProducerAPI(ContainerAPI):
    def __init__(self, container_name: str, port: int = 5000):
        super().__init__(container_type='producer', container_name=container_name, port=port)

    def validate_config(self, config):
        # Reuse existing validator
        validate_config(config)
        return True

    def handle_command(self, command, params):
        global virtual_train, eval_virtual_train, logger
        
        if command == 'set_Mp_std':
            new_val = params['Mp_std']
            if virtual_train is None:
                logger.error("Error reseting Mp_std: Virtual trains not initialized yet")
                return f"Virtual trains not initialized yet"
            virtual_train.Mp_std = new_val
            eval_virtual_train.Mp_std = new_val
            logger.info(f"Set Mp_std to {new_val}")
            return f"Set Mp_std to {new_val}"
        
        elif command == 'set_Bp_std':
            new_val = params['Bp_std']
            if virtual_train is None:
                logger.error("Error reseting Bp_std: Virtual trains not initialized yet")
                return f"Virtual trains not initialized yet"
            virtual_train.Bp_std = new_val
            eval_virtual_train.Bp_std = new_val
            logger.info(f"Set Bp_std to {new_val}")
            return f"Set Bp_std to {new_val}"

        elif command == 'set_eval_Mp_std':
            new_val = params['eval_Mp_std']
            if eval_virtual_train is None:
                logger.error("Error resetting eval_Mp_std: Virtual trains not initialized yet")
                return f"Virtual trains not initialized yet"
            eval_virtual_train.Mp_std = new_val
            logger.info(f"Set eval_Mp_std to {new_val}")
            return f"Set eval_Mp_std to {new_val}"

        elif command == 'set_eval_Bp_std':
            new_val = params['eval_Bp_std']
            if eval_virtual_train is None:
                logger.error("Error resetting eval_Bp_std: Virtual trains not initialized yet")
                return f"Virtual trains not initialized yet"
            eval_virtual_train.Bp_std = new_val
            logger.info(f"Set eval_Bp_std to {new_val}")
            return f"Set eval_Bp_std to {new_val}"



    def handle_start(self, data):
        global api_config
        self.logger.info("Main start command received!")
        if not self.config:
            raise ValueError("Not configured")
        with api_lock:
            api_config.update(self.config)
        success, message = start_producer_threads(api_config)
        if not success:
            raise RuntimeError(message)
        self.logger.info(f"{message}")
        return {"vehicle": api_config.get('vehicle_name'), "message": message}

    def handle_stop(self, data):
        self.logger.info("Main stop command received!")
        success, message = stop_producer_threads()
        # Make idempotent: treat already-stopped as success
        if not success:
            normalized = str(message).lower()
            if "not running" in normalized or "already" in normalized:
                self.logger.info(f"Already stopped! {message}")
                return {"status": "already_stopped", "message": message}
            raise RuntimeError(message)
        self.logger.info(f"{message}")
        return {"message": message}

    def get_detailed_status(self):
        main_status =  {
            "running": api_running,
            "vehicle": api_config.get('vehicle_name'),
            "records_produced": produced_records,
            "anomalies_produced": produced_anomalies,
            "diagnostics_produced": produced_diagnostics,
            "under_attack": UNDER_ATTACK,
            "config": api_config
        }
        self.logger.info(f"Main status requested: {main_status}")
        return main_status

def main():
    global VEHICLE_NAME, MANAGER_PORT, UNDER_ATTACK, attack_lock
    global producer, admin_client, logger, anomaly_generators, diagnostics_generators
    global anomaly_thread, diagnostics_thread, stop_threads, train_monitor
    global api_config

    # Load configuration from environment variables
    config = load_config_from_environment()
    
    # Validate configuration
    try:
        validate_config(config)
    except ValueError as e:
        print(f"Configuration error: {e}")
        return 1
    
    # Set global variables from config
    VEHICLE_NAME = config['vehicle_name']

    UNDER_ATTACK = False
    
    # Store config for API
    api_config = config.copy()

    # Configure logging
    logging.basicConfig(
        format='%(name)s-%(levelname)s-%(message)s', 
        level=str(config['logging_level']).upper()
    )
    logger = logging.getLogger(f'[{VEHICLE_NAME}_PROD]')
    
    # Log configuration summary
    logger.info(f"Starting producer for vehicle: {VEHICLE_NAME}")
    logger.info(f"Kafka broker: {config['kafka_broker']}")
    
    # Configure no proxy if needed
    if os.getenv('no_proxy_host'):
        configure_no_proxy()
    
    # Create Kafka producer
    conf_prod = {
        'bootstrap.servers': config['kafka_broker'],
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': lambda x, ctx: json.dumps(x).encode('utf-8')
    }
    producer = SerializingProducer(conf_prod)

    # Create Kafka admin client for topic lifecycle management
    admin_client = AdminClient({'bootstrap.servers': config['kafka_broker']})

    # Register cleanup so topics are deleted even on unclean shutdown
    atexit.register(cleanup_kafka)

    # Create attack object
    attack_lock = threading.Lock()

    logger.info(f"Setting up producing threads for vehicle: {VEHICLE_NAME}")

    # Create API using generic ContainerAPI subclass
    api = ProducerAPI(container_name=VEHICLE_NAME, port=5000)
    # Preload with merged, validated config
    api.validate_config(config)
    with api_lock:
        api_config.update(config)
    api.config.update(config)
    api.configured = True
    
    # Create Flask app for backdoor (existing functionality)
    backdoor_app = Flask(f'{VEHICLE_NAME}_backdoor')
    
    @backdoor_app.route('/start-attack', methods=['POST'])
    def start_attack():
        global UNDER_ATTACK, attack_thread
        logger.info("Received start attack request!")
        with attack_lock:
            if not UNDER_ATTACK:
                """
                attack_thread = threading.Thread(target=attack.start_attack)
                attack_thread.daemon = True
                attack_thread.start()
                """
                UNDER_ATTACK = True
                # train_monitor.reset()
                logger.info("Attack Launched!")
                return 'Attack launched', 200
            else:
                logger.info("Already under Attack!")
                return 'Already under attack!!', 400
    
    @backdoor_app.route('/stop-attack', methods=['POST'])
    def stop_attack():
        global UNDER_ATTACK, attack_thread
        logger.info("Received stop attack request!")
        with attack_lock:
            if UNDER_ATTACK:
                # attack.alive = False
                # attack_thread.join(1)
                UNDER_ATTACK = False
                # train_monitor.reset()
                logger.info("Attack stopped!")
                return 'Attack stopped', 200
            else:
                logger.info("Did nothing! Wasn't under attack!!")
                return 'Wasn\'t under attack!!', 400

    # Start Flask threads
    api_thread = threading.Thread(
        target=api.run,
        kwargs={}
    )
    api_thread.daemon = True
    api_thread.start()
    logger.info(f"Producer API started on port 5000")
    
    backdoor_thread = threading.Thread(
        target=backdoor_app.run, 
        kwargs={'host': '0.0.0.0', 'port': config['bot_port']}
    )
    backdoor_thread.daemon = True
    backdoor_thread.start()
    logger.info(f"Backdoor installed at {config['bot_port']}")
    
    # Suppress Flask logs
    flask_logger = logging.getLogger('werkzeug')
    flask_logger.name = f"[{VEHICLE_NAME}_BOT]"
    def custom_handle(record):
        return
    flask_logger.handle = custom_handle

    # Set up signal handlers
    signal.signal(signal.SIGINT, lambda sig, frame: signal_handler(sig, frame))
    signal.signal(signal.SIGTERM, lambda sig, frame: signal_handler(sig, frame))
    
    # Main loop
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
        stop_producer_threads()

if __name__ == '__main__':
    main()

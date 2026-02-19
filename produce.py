import pandas as pd
import numpy as np
import time
from scipy.stats import lognorm
import pickle
import threading
import time
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
import logging
import json
import argparse
import signal
import os
import requests
import subprocess
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


def adjust_probability(index, probability, main_classes, main_prob, low_prob):
    """
    Normalize the probabilities of classes based on given classes.
    This function adjusts the probabilities of classes by separating them into
    "main classes" (the ones specified in the config file)
    and "low probability classes" (the rest). The first group will have the 80% of probability,
    while the second group will have the remaining 20% of probability.
    """
    if index in main_classes:
        return probability * 0.8 / main_prob
    else:
        return probability * 0.2 / low_prob


def normalize_anomaly_probabilities(anomaly_classes):
    global anomaly_probabilities
    low_prob_classes = [x for x in range(0,19) if x not in anomaly_classes]
    
    main_anomaly_probabilities = anomaly_probabilities[anomaly_probabilities.index.isin(anomaly_classes)]
    main_prob = main_anomaly_probabilities['probability'].sum()

    low_anomaly_probabilities = anomaly_probabilities[anomaly_probabilities.index.isin(low_prob_classes)]
    low_prob = low_anomaly_probabilities['probability'].sum()

    anomaly_probabilities['probability'] = anomaly_probabilities.apply(
        lambda row: adjust_probability(
            row.name, row['probability'], anomaly_classes, main_prob, low_prob), axis=1)


def normalize_diagnostics_probabilities(diagnostics_classes):
    global diagnostics_probabilities
    low_prob_classes = [x for x in range(0,15) if x not in diagnostics_classes]
    # Filter for the subset of clusters
    main_diagnostics_probabilities = diagnostics_probabilities[diagnostics_probabilities.index.isin(diagnostics_classes)]
    main_prob = main_diagnostics_probabilities['probability'].sum()

    low_diagnostics_probabilities = diagnostics_probabilities[diagnostics_probabilities.index.isin(low_prob_classes)]
    low_prob = low_diagnostics_probabilities['probability'].sum()

    diagnostics_probabilities['probability'] = diagnostics_probabilities.apply(
        lambda row: adjust_probability(
            row.name, row['probability'], diagnostics_classes, main_prob, low_prob), axis=1)


def get_anomaly_generators_dict(anomaly_classes):
    normalize_anomaly_probabilities(anomaly_classes)
    anomaly_generators = {}
    for anomaly_class in range(0,19):
        with open(os.path.join(BASE_DIR, 'generators', 'anomalies', f'copula_anomalie_cluster_{anomaly_class}.pkl'), 'rb') as f:
            anomaly_generators[anomaly_class] = pickle.load(f)
    return anomaly_generators


def get_diagnostics_generators_dict(diagnostics_classes):
    normalize_diagnostics_probabilities(diagnostics_classes)
    diagnostics_generators = {}
    for diagnostics_class in range(0,15):
        with open(os.path.join(BASE_DIR, 'generators', 'diagnostics', f'copula_normal_cluster_{diagnostics_class}.pkl'), 'rb') as f:
            diagnostics_generators[diagnostics_class] = pickle.load(f)
    return diagnostics_generators


def signal_handler(sig, frame):
    global stop_threads
    logger.debug(f"Received signal {sig}. Gracefully stopping {VEHICLE_NAME} producer.")
    stop_threads = True


def configure_no_proxy():
    os.environ['no_proxy'] = os.environ.get('no_proxy', '') + f",{HOST_IP}"


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
    
    with api_lock:
        if api_running:
            return False, "Producer is already running"


        virtual_train = Train(argparse.Namespace(**config))
        eval_virtual_train = Train(argparse.Namespace(**config))
        
        """
        # Ensure generators are loaded based on current config
        if thread_args.anomaly_classes != list(range(0, 19)):
            anomaly_generators = get_anomaly_generators_dict(thread_args.anomaly_classes)
        if thread_args.diagnostics_classes != list(range(0, 15)):
            diagnostics_generators = get_diagnostics_generators_dict(thread_args.diagnostics_classes)
        """
        
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
        stop_threads = False  # Reset for next start
        
        return True, "Producer stopped successfully"

class ProducerAPI(ContainerAPI):
    def __init__(self, container_name: str, port: int = 5000):
        super().__init__(container_type='producer', container_name=container_name, port=port)

    def validate_config(self, config):
        # Reuse existing validator
        validate_config(config)
        return True

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
    global producer, logger, anomaly_generators, diagnostics_generators
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

    # Create attack object
    attack_lock = threading.Lock()
    
    """
    attack = Attack(
        target_ip=config['target_ip'],
        target_port=config['target_port'],
        duration=config['duration'],
        packet_size=config['packet_size'],
        delay=config['delay']
    )
    """

    logger.info(f"Setting up producing threads for vehicle: {VEHICLE_NAME}")
    
    """
    # Load generators if needed
    if config['anomaly_classes'] != list(range(0, 19)):
        anomaly_generators = get_anomaly_generators_dict(config['anomaly_classes'])
    if config['diagnostics_classes'] != list(range(0, 15)):
        diagnostics_generators = get_diagnostics_generators_dict(config['diagnostics_classes'])
    

    # Create train monitor
    train_monitor = TrainMonitor(argparse.Namespace(**config))
    """

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
                logger.info("Did nothing! Wasn\'t under attack!!")
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
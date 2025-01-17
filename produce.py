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


# load the copula objects later:
with open('copula_anomalie.pkl', 'rb') as f:
    copula_anomalie = pickle.load(f)

with open('copula_normali.pkl', 'rb') as f:
    copula_normali = pickle.load(f)

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


def parse_int_list(arg):
    # Split the input string by commas and convert each element to int
    try:
        return [int(x) for x in arg.split(',')]
    except ValueError:
        raise argparse.ArgumentTypeError("Arguments must be integers separated by commas")


def produce_message(data, topic_name):
    """
    Produce a message to Kafka for a specific sensor type.

    Args:
        data (dict): The data to be sent as a message.
        topic_name (str): The Kafka topic to which the message will be sent.
    """
    try:
        producer.produce(topic=topic_name, value=data)  # Send the message to Kafka
        producer.flush()  # Ensure the message is immediately sent
        logger.info(f"sent a message to {topic_name}")
    except Exception as e:
        print(f"Error while producing message to {topic_name} : {e}")


def sample_anomaly_from_global():
    return copula_anomalie.sample(1)


def sample_anomaly_from_clusters():
    cluster_to_sample_from = np.random.choice(
        anomaly_probabilities['cluster'],
        size=1, 
        p=anomaly_probabilities['probability'])
    return anomaly_generators[cluster_to_sample_from[0]].sample(1)


def thread_anomalie(args):
    media_durata_anomalie = args.mu_anomalies * args.alpha
    sigma_anomalie = 1 * args.beta
    lognormal_anomalie = lognorm(s=sigma_anomalie, scale=np.exp(np.log(media_durata_anomalie)))
    topic_name = f"{VEHICLE_NAME}_anomalies"

    if args.anomaly_classes == list(range(0,15)):
        sample_anomaly_function = sample_anomaly_from_global
    else:
        sample_anomaly_function = sample_anomaly_from_clusters

    while True:
        synthetic_anomalie = sample_anomaly_function()
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
        # print(f"Nuova anomalia generata: {synthetic_anomalie}")
        # Convert data to JSON and send it to Kafka
        data_to_send = synthetic_anomalie.iloc[0].to_dict()
        data_to_send['Timestamp'] = str(data_to_send['Timestamp'])
        data_to_send['Timestamp chiusura'] = str(data_to_send['Timestamp chiusura'])
        produce_message(data_to_send, topic_name)
        time.sleep(durata_anomalia[0])


def sample_normal_from_global():
    return copula_normali.sample(1)


def sample_normal_from_clusters():
    cluster_to_sample_from = np.random.choice(
        diagnostics_probabilities['cluster'],
        size=1, 
        p=diagnostics_probabilities['probability'])
    return diagnostics_generators[cluster_to_sample_from[0]].sample(1)


def thread_normali(args):
    media_durata_normali = args.mu_normal * args.alpha
    sigma_normali = 1 * args.beta
    lognormal_normali = lognorm(s=sigma_normali, scale=np.exp(np.log(media_durata_normali)))
    topic_name = f"{VEHICLE_NAME}_normal_data"

    if args.diagnostics_classes == list(range(0,15)):
        sample_normal_function = sample_normal_from_global
    else:
        sample_normal_function = sample_normal_from_clusters

    while True:
        synthetic_normali = copula_normali.sample(1)
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
        produce_message(data_to_send, topic_name)
        time.sleep(durata_normale[0])


def normalize_anomaly_probabilities(anomaly_classes):
    global anomaly_probabilities
    # Filter for the subset of clusters
    anomaly_probabilities = anomaly_probabilities[anomaly_probabilities.index.isin(anomaly_classes)]
    total_prob = anomaly_probabilities['probability'].sum()
    # Renormalize probabilities for the subset
    normalized_probabilities = anomaly_probabilities['probability'] / total_prob
    anomaly_probabilities['probability'] = normalized_probabilities


def normalize_diagnostics_probabilities(diagnostics_classes):
    global diagnostics_probabilities
    # Filter for the subset of clusters
    diagnostics_probabilities = diagnostics_probabilities[diagnostics_probabilities.index.isin(diagnostics_classes)]
    total_prob = diagnostics_probabilities['probability'].sum()
    # Renormalize probabilities for the subset
    normalized_probabilities = diagnostics_probabilities['probability'] / total_prob
    diagnostics_probabilities['probability'] = normalized_probabilities


def get_anomaly_generators_dict(anomaly_classes):
    normalize_anomaly_probabilities(anomaly_classes)
    anomaly_generators = {}
    for anomaly_class in anomaly_classes:
        with open(f'generators/anomalies/copula_anomalie_cluster_{anomaly_class}.pkl', 'rb') as f:
            anomaly_generators[anomaly_class] = pickle.load(f)
    return anomaly_generators


def get_diagnostics_generators_dict(diagnostics_classes):
    normalize_diagnostics_probabilities(diagnostics_classes)
    diagnostics_generators = {}
    for diagnostics_class in diagnostics_classes:
        with open(f'generators/diagnostics/copula_normal_cluster_{diagnostics_class}.pkl', 'rb') as f:
            diagnostics_generators[diagnostics_class] = pickle.load(f)
    return diagnostics_generators


def main():
    global VEHICLE_NAME, KAFKA_BROKER
    global producer, logger, anomaly_generators, diagnostics_generators

    parser = argparse.ArgumentParser(description='Kafka Producer for Synthetic Vehicle Data')
    parser.add_argument('--vehicle_name', type=str, required=True, help='Name of the vehicle')        
    parser.add_argument('--container_name', type=str, default='GENERIC_PRODUCER', help='Name of the container')
    parser.add_argument('--kafka_broker', type=str, default='kafka:9092', help='Kafka broker URL')
    parser.add_argument('--logging_level', type=str, default='INFO', help='Logging level')
    parser.add_argument('--mu_anomalies', type=float, default=157, help='Mu parameter (mean of the mean interarrival times of anomalies)')
    parser.add_argument('--mu_normal', type=float, default=115, help='Mu parameter (mean of the mean interarrival times of normal data)')
    parser.add_argument('--alpha', type=float, default=0.2, help='Alpha parameter (scaling factor of the mean interarrival times of both anomalies and normal data)')
    parser.add_argument('--beta', type=float, default=1.9, help='Beta parameter (std dev of interarrival times of both anomalies and normal data)')
    parser.add_argument('--anomaly_classes',  type=parse_int_list, default=list(range(0,15)))
    parser.add_argument('--diagnostics_classes', type=parse_int_list, default=list(range(0,15)))

    args = parser.parse_args()

    VEHICLE_NAME = args.vehicle_name

    logger = logging.getLogger(args.container_name)
    logger.setLevel(str(args.logging_level).upper())

    conf_prod = {
        'bootstrap.servers': args.kafka_broker,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': lambda x, ctx: json.dumps(x).encode('utf-8')
    }
    producer = SerializingProducer(conf_prod)

    logger.debug(f"Setting up threads for vehicle: {VEHICLE_NAME}")
    vehicle_args=argparse.Namespace(
        mu_anomalies=args.mu_anomalies,
        mu_normal=args.mu_normal,
        alpha=args.alpha,
        beta=args.beta,
        anomaly_classes=args.anomaly_classes,
        diagnostics_classes=args.diagnostics_classes
    )

    if args.anomaly_classes != list(range(0,15)):
        anomaly_generators = get_anomaly_generators_dict(args.anomaly_classes)
    if args.diagnostics_classes != list(range(0,15)):
        diagnostics_generators = get_diagnostics_generators_dict(args.diagnostics_classes)

    thread1 = threading.Thread(target=thread_anomalie, args=(vehicle_args,))
    thread2 = threading.Thread(target=thread_normali, args=(vehicle_args,))

    # Set daemon to True
    thread1.daemon = True
    thread2.daemon = True

    # Add threads to the list
    #threads.extend([thread1,thread2])

    # Start threads
    logging.info(f"Starting threads for vehicle: {VEHICLE_NAME}")
    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()


if __name__ == '__main__':
    main()
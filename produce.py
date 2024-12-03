import pandas as pd
import numpy as np
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from scipy.stats import lognorm
import pickle
import threading
import time
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
import logging
import os
import json
import argparse


# Configure logging for detailed information
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables for Kafka broker and topic name
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
VEHICLE_NAME=os.getenv('VEHICLE_NAME')
CONTAINER_NAME = os.getenv('CONTAINER_NAME', 'generic_producer')
# Validate that KAFKA_BROKER and TOPIC_NAME are set
if not KAFKA_BROKER:
    raise ValueError("Environment variable KAFKA_BROKER is missing.")
if not VEHICLE_NAME:
    raise ValueError("Environment variable VEHICLE_NAME is missing.")

# Kafka producer configuration
conf_prod = {
    'bootstrap.servers': KAFKA_BROKER,
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': lambda x, ctx: json.dumps(x).encode('utf-8')
}
producer = SerializingProducer(conf_prod)

# load the copula objects later:
with open('copula_anomalie.pkl', 'rb') as f:
    copula_anomalie = pickle.load(f)

with open('copula_normali.pkl', 'rb') as f:
    copula_normali = pickle.load(f)

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
        logging.info(f"{CONTAINER_NAME} sent a message to {topic_name}")
    except Exception as e:
        print(f"Error while producing message to {topic_name} : {e}")


def thread_anomalie(args):
  media_durata_anomalie = args.mu_anomalies * args.alpha
  sigma_anomalie = 1 * args.beta
  lognormal_anomalie = lognorm(s=sigma_anomalie, scale=np.exp(np.log(media_durata_anomalie)))

  topic_name = f"{VEHICLE_NAME}_anomalies"

  while True:
    synthetic_anomalie = copula_anomalie.sample(1)
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


def thread_normali(args):
  media_durata_normali = args.mu_normal * args.alpha
  sigma_normali = 1 * args.beta
  lognormal_normali = lognorm(s=sigma_normali, scale=np.exp(np.log(media_durata_normali)))

  topic_name = f"{VEHICLE_NAME}_normal_data"

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


def main():
    parser = argparse.ArgumentParser(description='Kafka Producer for Synthetic Vehicle Data')
    parser.add_argument('--mu_anomalies', type=float, default=157, help='Mu parameter (mean of the mean interarrival times of anomalies)')
    parser.add_argument('--mu_normal', type=float, default=115, help='Mu parameter (mean of the mean interarrival times of normal data)')
    parser.add_argument('--alpha', type=float, default=0.2, help='Alpha parameter (scaling factor of the mean interarrival times of both anomalies and normal data)')
    parser.add_argument('--beta', type=float, default=1.9, help='Beta parameter (std dev of interarrival times of both anomalies and normal data)')
    #parser.add_argument('--vehicle_name', type=str, default='e700_4801', help='Comma-separated list of vehicle names')

    args = parser.parse_args()

    #vehicle_names=args.vehicle_name.split(',')

    #threads=[]

    logging.info(f"Setting up threads for vehicle: {VEHICLE_NAME}")
    vehicle_args=argparse.Namespace(
        mu_anomalies=args.mu_anomalies,
        mu_normal=args.mu_normal,
        alpha=args.alpha,
        beta=args.beta,
        #vehicle_name=VEHICLE_NAME
    )

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
